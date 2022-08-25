import csv
from io import TextIOWrapper
import requests
from pathlib import Path
from zipfile import ZipFile
from lxml import etree, html
from normality import slugify
from urllib.parse import urljoin
from contextlib import contextmanager
from typing import BinaryIO, Dict, List, Optional, Tuple
from followthemoney import model
from zavod import Zavod, init_context
from zavod.parse import remove_namespace

LEI = "http://www.gleif.org/data/schema/leidata/2016"
RR = "http://www.gleif.org/data/schema/rr/2016"

CAT_URL = "https://www.gleif.org/en/lei-data/gleif-concatenated-file/download-the-concatenated-file"
BIC_URL = "https://www.gleif.org/en/lei-data/lei-mapping/download-bic-to-lei-relationship-files"
ISIN_URL = "https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files"

# TODO: addresses!

RELATIONSHIPS: Dict[str, Tuple[str, str, str]] = {
    "IS_FUND-MANAGED_BY": ("Directorship", "organization", "director"),
    "IS_SUBFUND_OF": ("Directorship", "organization", "director"),
    "IS_DIRECTLY_CONSOLIDATED_BY": ("Ownership", "asset", "owner"),
    "IS_ULTIMATELY_CONSOLIDATED_BY": ("Ownership", "asset", "owner"),
    "IS_INTERNATIONAL_BRANCH_OF": ("Ownership", "asset", "owner"),
    "IS_FEEDER_TO": ("UnknownLink", "subject", "object"),
}


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


def lei_id(lei: str) -> str:
    return f"lei-{lei}"


def parse_date(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return text.split("T")[0]


def fetch_bic_mapping(context: Zavod) -> Path:
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
    return context.fetch_resource("bic_lei.csv", csv_url)


def fetch_isin_mapping(context: Zavod) -> Path:
    res = requests.get(ISIN_URL)
    doc = html.fromstring(res.text)
    mapping_url = None
    for link in doc.findall(".//a"):
        url = urljoin(BIC_URL, link.get("href"))
        if "https://mapping.gleif.org/api/v2/isin-lei/" in url:
            mapping_url = url
            break
    if mapping_url is None:
        raise RuntimeError("No ISIN mapping file found!")
    return context.fetch_resource("isin.zip", mapping_url)


def fetch_cat_file(context: Zavod, url_part: str, name: str) -> Optional[Path]:
    res = requests.get(CAT_URL)
    doc = html.fromstring(res.text)
    for link in doc.findall(".//a"):
        url = urljoin(BIC_URL, link.get("href"))
        if url_part in url:
            return context.fetch_resource(name, url)
    return None


def fetch_lei_file(context: Zavod) -> Path:
    path = fetch_cat_file(context, "/concatenated-files/lei2/get/", "lei.zip")
    if path is None:
        raise RuntimeError("Cannot find cat LEI2 file!")
    return path


def fetch_rr_file(context: Zavod) -> Path:
    path = fetch_cat_file(context, "/concatenated-files/rr/get/", "rr.zip")
    if path is None:
        raise RuntimeError("Cannot find cat RR file!")
    return path


@contextmanager
def read_zip_file(context: Zavod, path: Path):
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            context.log.info("Reading: %s in %s" % (name, path))
            with zip.open(name, "r") as fh:
                yield fh


def load_bic_mapping(context: Zavod) -> Dict[str, List[str]]:
    csv_path = fetch_bic_mapping(context)
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


def load_isin_mapping(context: Zavod) -> Dict[str, List[str]]:
    zip_path = fetch_isin_mapping(context)
    mapping: Dict[str, List[str]] = {}
    with read_zip_file(context, zip_path) as fh:
        textfh = TextIOWrapper(fh, encoding='utf-8')
        for row in csv.DictReader(textfh):
            lei = row.get("LEI")
            if lei is None:
                raise RuntimeError("No LEI in BIC/LEI mapping")
            mapping.setdefault(lei, [])
            isin = row.get("ISIN")
            if isin is not None:
                mapping[lei].append(isin)
    return mapping


def parse_lei_file(context: Zavod, fh: BinaryIO) -> None:
    elfs = load_elfs()
    bics = load_bic_mapping(context)
    isins = load_isin_mapping(context)
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag="{%s}LEIRecord" % LEI)):
        if idx > 0 and idx % 10000 == 0:
            context.log.info("Parse LEIRecord: %d..." % idx)
        elc = remove_namespace(el)
        proxy = model.make_entity("Company")
        lei = elc.findtext("LEI")
        if lei is None:
            continue
        proxy.id = lei_id(lei)
        entity = elc.find("Entity")
        if entity is None:
            continue
        proxy.add("name", entity.findtext("LegalName"))
        proxy.add("jurisdiction", entity.findtext("LegalJurisdiction"))
        proxy.add("status", entity.findtext("EntityStatus"))
        create_date = parse_date(entity.findtext("EntityCreationDate"))
        proxy.add("incorporationDate", create_date)
        authority = entity.find("RegistrationAuthority")
        if authority is not None:
            reg_id = authority.findtext("RegistrationAuthorityEntityID")
            proxy.add("registrationNumber", reg_id)
            
        proxy.add("swiftBic", bics.get(lei))
        proxy.add("leiCode", lei, quiet=True)

        for isin in isins.get(lei, []):
            security = model.make_entity("Security")
            security.id = f"lei-isin-{isin}"
            security.add('isin', isin)
            security.add('issuer', proxy.id)
            context.emit(security)

        legal_form = entity.find("LegalForm")
        if legal_form is not None:
            code = legal_form.findtext("EntityLegalFormCode")
            proxy.add("legalForm", elfs.get(code))
            proxy.add("legalForm", legal_form.findtext("OtherLegalForm"))

        registration = elc.find("Registration")
        if registration is not None:
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
            context.emit(succession)

        el.clear()
        context.emit(proxy)

    if idx == 0:
        raise RuntimeError("No entities!")


def parse_rr_file(context: Zavod, fh: BinaryIO):
    tag = "{%s}RelationshipRecord" % RR
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag=tag)):
        if idx > 0 and idx % 10000 == 0:
            context.log.info("Parse RelationshipRecord: %d..." % idx)
        elc = remove_namespace(el)
        # print(elc)
        rel = elc.find("Relationship")
        if rel is None:
            continue
        rel_type = rel.findtext("RelationshipType")
        rel_data = RELATIONSHIPS.get(rel_type)
        if rel_data is None:
            context.log.warn("Unknown relationship: %s", rel_type)
            continue
        rel_schema, start_prop, end_prop = rel_data

        start_node = rel.find("StartNode")
        start_node_type = start_node.findtext("NodeIDType")
        if start_node_type != "LEI":
            context.log.warn("Unknown edge type", node_id_type=start_node_type)
            continue
        start_lei = start_node.findtext("NodeID")
        end_node = rel.find("EndNode")
        end_node_type = end_node.findtext("NodeIDType")
        if end_node_type != "LEI":
            context.log.warn("Unknown edge type", node_id_type=end_node_type)
            continue
        end_lei = end_node.findtext("NodeID")

        proxy = model.make_entity(rel_schema)
        rel_id = slugify(rel_type, sep="-")
        proxy.id = f"lei-{start_lei}-{rel_id}-{end_lei}"
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
                context.log.warn("Unknown rel quantifier", amount=amount, units=units)

        el.clear()
        context.emit(proxy)

    if idx == 0:
        raise RuntimeError("No relationships!")


def parse(context: Zavod):
    lei_file = fetch_lei_file(context)
    with read_zip_file(context, lei_file) as fh:
        parse_lei_file(context, fh)

    rr_file = fetch_rr_file(context)
    with read_zip_file(context, rr_file) as fh:
        parse_rr_file(context, fh)


if __name__ == "__main__":
    with init_context("gleif", "lei") as context:
        parse(context)
