import os
import requests
from tqdm import tqdm
import xml.etree.ElementTree as ElementTree
import zipfile


def download_file(link, path):
    resp = requests.get(link, stream=True, verify=False)
    total = int(resp.headers.get('content-length', 0))
    # os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as file, tqdm(
            desc=path,
            total=total,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)


def download_list():
    url_list = "https://proverki.gov.ru/blob/erknm-opendata/list.xml"
    path_list = "./downloads/list/list.xml"
    download_file(url_list, path_list)


def download_file_xml_inspection_versions(file_name):
    file_link = f'https://proverki.gov.ru/blob/erknm-opendata/7710146102-{file_name}.xml'
    file_path = f'./downloads/inspection_versions/{file_name}.xml'
    download_file(file_link, file_path)


def parse_inspection_versions_to_url_last_zip(file_name):
    root_node = ElementTree.parse(f'./downloads/inspection_versions/{file_name}.xml').getroot()
    list_source = root_node.findall('data/dataversion/source')
    element = list_source[-1]
    last_url = element.text
    return last_url


def download_file_inspection_zip(file_name):
    link = parse_inspection_versions_to_url_last_zip(file_name)
    path_zips = f'./downloads/zips/{file_name}.zip'
    download_file(link, path_zips)


def unzip_file(file_name):
    path_from = f'./downloads/zips/{file_name}.zip'
    path_to = f'./downloads/unzips/{file_name}'
    with zipfile.ZipFile(path_from, 'r') as zip_file:
        zip_file.extractall(path_to)


def download_and_unzip_inspection_zip(file_name):
    download_file_inspection_zip(file_name)
    unzip_file(file_name)


def download_and_unzip_inspection(file_name):
    download_file_xml_inspection_versions(file_name)
    download_and_unzip_inspection_zip(file_name)


def tag_without_namespace(tag):
    return tag.split('}')[1]


def get_attribute_part(attribute, part):
    return f'{attribute}'.split("'")[part]


def get_attribute_key(attribute):
    attr_key = 1
    return get_attribute_part(attribute, attr_key)


def get_attribute_value(attribute):
    attr_value = 3
    return get_attribute_part(attribute, attr_value)


def get_element_attribute_keys_for_csv(element):
    tag = tag_without_namespace(element.tag)
    out_string = ''
    for item in element.items():
        out_string = f'{out_string};'
        if tag != '':
            out_string = f'{out_string}{tag}_'
        out_string = f'{out_string}{get_attribute_key(item)}'
    return out_string


def get_inspection_structure():
    return {
        'CLASSIFICATION': '',
        'CREATION_SOURCE': '',
        'STATUS': '',
        'STATUS_KEY': '',
        'ERPID': '',
        'KO_LEVEL': '',
        'SUPERVISION_ID': '',
        'START_DATE': '',
        'STOP_DATE': '',
        'IS_CHECKLISTS_USED': '',
        'TYPE_NAME': '',
        'IS_HAS_COLLABORATING_ORGANIZATION': '',
        'RETURN_SELECTION': '',
        'DURATION_DAYS': '',
        'DURATION_HOURS': '',
        'IS_REMOTE': '',
        'NOTE_REMOTE': '',
        'DOCUMENT_REQUEST_DATE': '',
        'DOCUMENT_RESPONSE_DATE': '',
        'IS_PRESENCE': '',
        'IS_SELECTION': '',
        'WITH_VIDEO': ''
    }


gets_header_structure_functions = {
    'INSPECTION': get_inspection_structure()
}


def get_element_structure_by_tag(tag):
    return gets_header_structure_functions[tag]


def get_element_structure(element):
    return get_element_structure_by_tag(tag_without_namespace(element.tag))


def get_element_attribute_values_for_csv(element):
    element_structure = get_element_structure(element)
    out_string = ''
    for item in element.items():
        element_structure[get_attribute_key(item)] = get_attribute_value(item)
    for entry in element_structure:
        out_string = f'{out_string};{element_structure[entry]}'

    return out_string


def get_header_inspection_for_csv():
    out_string = ''
    for entry in get_inspection_structure():
        out_string = f'{out_string};Inspection_{entry}'
    return out_string


def get_element_header_for_csv_by_tag(tag):
    out_string = ''
    for entry in get_element_structure_by_tag(tag):
        out_string = f'{out_string};{tag}_{entry}'
    return out_string


def get_element_header_for_csv(element):
    return get_element_header_for_csv_by_tag(tag_without_namespace(element.tag))


def get_header_for_csv_by_root_node(root_node):
    header = 'ID'
    header = f'{header}{get_element_header_for_csv(root_node[0])}'
    return header


def get_header_for_csv():
    header = 'ID'
    header = f'{header}{get_header_inspection_for_csv()}'
    return header


def parse_inspection_xml_to_csv(dir_name):
    file_name = os.listdir(f'./downloads/unzips/{dir_name}')[-1]
    root_node = ElementTree.parse(f'./downloads/unzips/{dir_name}/{file_name}').getroot()
    id = 1

    """with open(f'./csvs/{dir_name}.csv', 'w') as the_file:
        the_file.write(f'{get_header_for_csv()}\n')"""
    with open(f'./csvs/{dir_name}.csv', 'w') as the_file:
        the_file.write(f'{get_header_for_csv_by_root_node(root_node)}\n')
    for inspection in root_node:
        line = f'{id}{get_element_attribute_values_for_csv(inspection)}'
        with open(f'./csvs/{dir_name}.csv', 'a') as the_file:
            the_file.write(f'{line}\n')
        id += 1


if __name__ == '__main__':
    file_type_inspection = "inspection"
    year = "2022"
    month = "7"
    file_name = f'{file_type_inspection}-{year}'
    if month != "":
        file_name = f'{file_name}-{month}'
    download_list()
    download_and_unzip_inspection(file_name)
    parse_inspection_xml_to_csv(file_name)
