import csv
import logging
import requests
from pathlib import Path
from zipfile import ZipFile
from lxml import etree, html
from normality import slugify
from urllib.parse import urljoin
from contextlib import contextmanager
from typing import Generator, BinaryIO, Dict, List, Optional, Tuple

from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.cli.util import write_object

log = logging.getLogger("gleifparse")
DATA = Path("data/").resolve()
LEI = "http://www.gleif.org/data/schema/leidata/2016"
RR = "http://www.gleif.org/data/schema/rr/2016"

CAT_URL = "https://www.gleif.org/en/lei-data/gleif-concatenated-file/download-the-concatenated-file"
BIC_URL = "https://www.gleif.org/en/lei-data/lei-mapping/download-bic-to-lei-relationship-files"

# TODO: addresses!

RELATIONSHIPS: Dict[str, Tuple[str, str, str]] = {
    "IS_FUND-MANAGED_BY": ("Directorship", "organization", "director"),
    "IS_SUBFUND_OF": ("Directorship", "organization", "director"),
    "IS_DIRECTLY_CONSOLIDATED_BY": ("Ownership", "asset", "owner"),
    "IS_ULTIMATELY_CONSOLIDATED_BY": ("Ownership", "asset", "owner"),
    "IS_INTERNATIONAL_BRANCH_OF": ("Ownership", "asset", "owner"),
    "IS_FEEDER_TO": ("UnknownLink", "subject", "object"),
}


def fetch_file(url: str, name: str) -> Path:
    out_path = DATA / name
    if out_path.exists():
        return out_path
    log.info("Fetching: %s", url)
    with requests.get(url, stream=True) as res:
        res.raise_for_status()
        with open(out_path, "wb") as fh:
            for chunk in res.iter_content(chunk_size=8192):
                fh.write(chunk)
    return out_path


def load_elfs() -> Dict[str, str]:
    names = {}
    # https://www.gleif.org/en/about-lei/code-lists/iso-20275-entity-legal-forms-code-list#
    with open("ref/elf-codes-1.4.1.csv", "r") as fh:
        for row in csv.DictReader(fh):
            data = {slugify(k, sep="_"): v for k, v in row.items()}
            label = data["entity_legal_form_name_local_name"].strip()
            if len(label):
                names[data["elf_code"]] = label

    return names


def remove_namespace(el):
    for elem in el.getiterator():
        elem.tag = etree.QName(elem).localname
    etree.cleanup_namespaces(el)
    return el


def lei_id(lei: str) -> str:
    return f"lei-{lei}"


def parse_date(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return text.split("T")[0]


def fetch_bic_mapping() -> Path:
    res = requests.get(BIC_URL)
    doc = html.fromstring(res.text)
    csv_url = None
    for link in doc.findall(".//a"):
        if "download" in link.attrib:
            url = urljoin(BIC_URL, link.get("href"))
            if url.endswith(".csv"):
                csv_url = url
                break
    if csv_url is None:
        raise RuntimeError("No BIC/LEI mapping file found!")
    return fetch_file(csv_url, "bic_lei.csv")


def fetch_cat_file(url_part: str) -> Optional[Path]:
    res = requests.get(CAT_URL)
    doc = html.fromstring(res.text)
    for link in doc.findall(".//a"):
        url = urljoin(BIC_URL, link.get("href"))
        if url_part in url:
            return fetch_file(url, "lei.zip")
    return None


def fetch_lei_file() -> Path:
    path = fetch_cat_file("/concatenated-files/lei2/get/")
    if path is None:
        raise RuntimeError("Cannot find cat LEI2 file!")
    return path


def fetch_rr_file() -> Path:
    path = fetch_cat_file("/concatenated-files/rr/get/")
    if path is None:
        raise RuntimeError("Cannot find cat RR file!")
    return path


@contextmanager
def read_zip_xml(path: Path):
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            with zip.open(name, "r") as fh:
                yield fh


def load_bic_mapping() -> Dict[str, List[str]]:
    csv_path = fetch_bic_mapping()
    mapping: Dict[str, List[str]] = {}
    with open(csv_path, "r") as fh:
        for row in csv.DictReader(fh):
            lei = row.get("LEI")
            if lei is None:
                raise RuntimeError("No LEI in BIC/LEI mapping")
            mapping.setdefault(lei, [])
            bic = row.get("BIC")
            if bic is not None:
                mapping[lei].append(bic)
    return mapping


def parse_lei_file(fh: BinaryIO) -> Generator[EntityProxy, None, None]:
    elfs = load_elfs()
    bics = load_bic_mapping()
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag="{%s}LEIRecord" % LEI)):
        if idx > 0 and idx % 10000 == 0:
            log.info("Parse LEIRecord: %d...", idx)
        elc = remove_namespace(el)
        proxy = model.make_entity("Company")
        lei = elc.findtext("LEI")
        proxy.id = lei_id(lei)
        entity = elc.find("Entity")
        proxy.add("name", entity.findtext("LegalName"))
        proxy.add("jurisdiction", entity.findtext("LegalJurisdiction"))
        proxy.add("status", entity.findtext("EntityStatus"))
        create_date = parse_date(entity.findtext("EntityCreationDate"))
        proxy.add("incorporationDate", create_date)
        authority = entity.find("RegistrationAuthority")
        reg_id = authority.findtext("RegistrationAuthorityEntityID")
        proxy.add("registrationNumber", reg_id)
        proxy.add("swiftBic", bics.get(lei))
        proxy.add("leiCode", lei, quiet=True)

        legal_form = entity.find("LegalForm")
        code = legal_form.findtext("EntityLegalFormCode")
        proxy.add("legalForm", elfs.get(code))
        proxy.add("legalForm", legal_form.findtext("OtherLegalForm"))

        registration = elc.find("Registration")
        mod_date = parse_date(registration.findtext("LastUpdateDate"))
        proxy.add("modifiedAt", mod_date)
        # pprint(proxy.to_dict())

        successor = elc.find("SuccessorEntity")
        if successor is not None:
            succ_lei = successor.findtext("SuccessorLEI")
            succession = model.make_entity("Succession")
            succession.id = f"lei-succession-{lei}-{succ_lei}"
            succession.add("predecessor", lei)
            succession.add("successor", lei_id(succ_lei))
            yield succession

        el.clear()
        yield proxy

    if idx == 0:
        raise RuntimeError("No entities!")


def parse_rr_file(fh: BinaryIO) -> Generator[EntityProxy, None, None]:
    tag = "{%s}RelationshipRecord" % RR
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag=tag)):
        if idx > 0 and idx % 10000 == 0:
            log.info("Parse RelationshipRecord: %d...", idx)
        elc = remove_namespace(el)
        # print(elc)
        rel = elc.find("Relationship")
        rel_type = rel.findtext("RelationshipType")
        rel_data = RELATIONSHIPS.get(rel_type)
        if rel_data is None:
            log.warning("Unknown relationship: %s", rel_type)
            continue
        rel_schema, start_prop, end_prop = rel_data

        start_node = rel.find("StartNode")
        if start_node.findtext("NodeIDType") != "LEI":
            log.warning("Unknown edge type: %s", start_node.findtext("NodeIDType"))
            continue
        start_lei = start_node.findtext("NodeID")
        end_node = rel.find("EndNode")
        if end_node.findtext("NodeIDType") != "LEI":
            log.warning("Unknown edge type: %s", end_node.findtext("NodeIDType"))
            continue
        end_lei = end_node.findtext("NodeID")

        proxy = model.make_entity(rel_schema)
        proxy.id = f"lei-rel-{start_lei}-{end_lei}"
        proxy.add(start_prop, lei_id(start_lei))
        proxy.add(end_prop, lei_id(end_lei))
        proxy.add("role", rel_type.replace("_", " "))
        proxy.add("status", rel.findtext("RelationshipStatus"))

        for period in rel.findall(".//RelationshipPeriod"):
            period_type = period.findtext("PeriodType")
            if period_type == "RELATIONSHIP_PERIOD":
                proxy.add("startDate", parse_date(period.findtext("StartDate")))
                proxy.add("endDate", parse_date(period.findtext("EndDate")))

        for quant in rel.findall(".//RelationshipQuantifier"):
            amount = quant.findtext("QuantifierAmount")
            units = quant.findtext("QuantifierUnits")
            if units == "PERCENTAGE" or units is None:
                proxy.add("percentage", amount, quiet=True)
            else:
                log.warning("Unknown rel quantifier: %s %s", amount, units)

        el.clear()
        yield proxy


def parse():
    out_path = DATA / "export" / "gleif.json"
    out_path.parent.mkdir(exist_ok=True, parents=True)
    with open(out_path, "w") as out_fh:
        lei_file = fetch_lei_file()
        with read_zip_xml(lei_file) as fh:
            for proxy in parse_lei_file(fh):
                write_object(out_fh, proxy)

        rr_file = fetch_rr_file()
        with read_zip_xml(rr_file) as fh:
            for proxy in parse_rr_file(fh):
                write_object(out_fh, proxy)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parse()
