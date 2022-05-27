from typing import BinaryIO, Optional
from lxml import etree
from pprint import pprint

from ftmstore import get_dataset, Dataset
from followthemoney import model
from followthemoney.types import registry

LEI = "http://www.gleif.org/data/schema/leidata/2016"


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


def parse_lei_file(dataset: Dataset, fh: BinaryIO):
    bulk = dataset.bulk()
    for _, el in etree.iterparse(fh, tag="{%s}LEIRecord" % LEI):
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

        legal_form = entity.find("LegalForm")
        proxy.add("legalForm", legal_form.findtext("OtherLegalForm"))

        registration = elc.find("Registration")
        mod_date = parse_date(registration.findtext("LastUpdateDate"))
        proxy.add("modifiedAt", mod_date)
        # pprint(proxy.to_dict())

        successor = elc.find("SuccessorEntity")
        if successor is not None:
            succ_lei = successor.findtext("SuccessorLEI")
            succession = model.make_entity("Succession")
            succession.id = f"succession-{lei}-{succ_lei}"
            succession.add("predecessor", lei)
            succession.add("successor", lei_id(succ_lei))
            bulk.put(succession)

        el.clear()
        bulk.put(proxy)


if __name__ == "__main__":
    dataset = get_dataset("leidata", database_uri="sqlite:///data/ftm.store")
    dataset.delete()
    with open("data/20220518-gleif-concatenated-file-lei2.xml", "rb") as fh:
        parse_lei_file(dataset, fh)
