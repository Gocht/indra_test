import os
import pandas as pd
import hashlib
import logging
import xml.etree.ElementTree as ET
from functools import lru_cache

# config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# constants
OUTPUT_PATH = 'pd_output/'
INPUT_FILE = 'sample/data.xml'

@lru_cache(maxsize=1000)
def generate_id(root, prev=''):
    """ Generate a unique id for the element based on the element's tag, attributes, and text content. """
    content = ET.tostring(root).decode()
    return hashlib.sha1(''.join([prev, content]).encode()).hexdigest()

def process_element(element):
    """ Process an element and return a dictionary of its attributes and text content. """
    _ = {}
    for attr in element.items():
        _[f'{element.tag}_{attr[0]}'] = attr[1]
    if element.text.replace('\n', '').strip():
        _[element.tag] = element.text
    return _

def add_fk(fks, df):
    """ Add FK to dict from another df"""
    _ = {}
    for fk in fks:
        try:
            _[f'FK_{fk}'] = df[f'PK_{fk}'].iloc[0]
        except KeyError as e:
            _[f'FK_{fk}'] = df[f'FK_{fk}'].iloc[0]
    return _

def save(df, table_name):
    """ Save dataframe """
    try:
        path = os.path.join(OUTPUT_PATH, f'{table_name}.parquet')
        print('path')
        print(df.columns)
        df.to_parquet(
            path=path,
            # compression='snappy',
            index=False
        )
        logger.info(f'Saved {table_name} to {path}')
    except Exception as e:  # Catch any, TODO: make ir specific
        logger.error(f'Error saving {table_name} to {path}: {e}')
        raise


def persautopolicymodrq(root):
    fields = ['RequId', 'TransactionRequestDt', 'TransactionEffectiveDt']
    rows = []
    row = {}
    for element in fields:
        el = root.find(element)
        for _ in el.items():
            row[f'{el.tag}_{_[0]}'] = _[1]
        row[el.tag] = el.text
    row['PK_PersAutoPolicyModRq'] = generate_id(root)
    rows.append(row)
    return pd.DataFrame(rows)

def persautopolicymodrq_producer(root, prev_df):
    
    rows = []
    for element in root.findall('Producer'):
        row = {}
        row.update(process_element(element))
        row.update(add_fk(['PersAutoPolicyModRq'], prev_df))
        row['PK_PersAutoPolicyModRq_Producer'] = generate_id(root.find(element.tag), row['FK_PersAutoPolicyModRq'])
        rows.append(row)
    return pd.DataFrame(rows)

def persautopolicymodrq_producer_itemidinfo(root, prev_df):
    
    rows = []
    row = {}
    for element in root.find('ItemIdInfo').iter():
        row.update(process_element(element))
        row.update(add_fk(['PersAutoPolicyModRq_Producer', 'PersAutoPolicyModRq'], prev_df))
        row['PK_PersAutoPolicyModRq_Producer_ItemIdInfo'] = generate_id(root.find('ItemIdInfo'), row['FK_PersAutoPolicyModRq_Producer'])
    rows.append(row)
    return pd.DataFrame(rows)

def persautopolicymodrq_producer_generalpartyinfo(root, prev_df):
    rows = []
    for element in root.findall('GeneralPartyInfo'):
        row = {}
        row.update(process_element(element))
        row.update(add_fk(['PersAutoPolicyModRq_Producer', 'PersAutoPolicyModRq'], prev_df))
        row['PK_Producer_GeneralPartyInfo'] = generate_id(root.find(element.tag), prev_df['PK_PersAutoPolicyModRq_Producer_ItemIdInfo'].iloc[0])
        rows.append(row)
    return pd.DataFrame(rows)

def persautopolicymodrq_producer_producerinfo(root, prev_df):
    rows = []
    for el in root.findall('ProducerInfo'):
        row = {}
        for element in el.iter():
            row.update(process_element(element))
        row.update(add_fk(['PersAutoPolicyModRq_Producer', 'PersAutoPolicyModRq'], prev_df))
        row['PK_Producer_ProducerInfo'] = generate_id(el, prev_df['PK_Producer_GeneralPartyInfo'].iloc[0])
        rows.append(row)
    return pd.DataFrame(rows)

def persautopolicymodrq_producer_generalpartyinfo_nameinfo(root, prev_df):
    rows = []
    for element in root.findall('NameInfo'):
        row = {}
        row.update(process_element(element))
        row.update(add_fk(['PersAutoPolicyModRq_Producer', 'PersAutoPolicyModRq', 'Producer_GeneralPartyInfo'], prev_df))
        row['PK_GeneralPartyInfo_NameInfo'] = generate_id(element, row['FK_Producer_GeneralPartyInfo'])
        rows.append(row)
    return pd.DataFrame(rows)

def persautopolicymodrq_producer_generalpartyinfo_nameinfo_commlname(root, prev_df):
    excluded = ['NameInfo', 'TaxIdentity', 'TaxIdTypeCd', 'TaxId']
    rows = []
    for el in root.findall('NameInfo'):
        row = {}
        for element in el.iter():
            if element.tag in excluded: continue
            row.update(process_element(element))
        row.update(add_fk(['PersAutoPolicyModRq_Producer', 'PersAutoPolicyModRq', 'Producer_GeneralPartyInfo'], prev_df))
        row['FK_GeneralPartyInfo_NameInfo'] = generate_id(el, row['FK_Producer_GeneralPartyInfo'])
        row['PK_NameInfo_CommlName'] = generate_id(element, row['FK_GeneralPartyInfo_NameInfo'])
        rows.append(row)
    return pd.DataFrame(rows)

def persautopolicymodrq_producer_generalpartyinfo_nameinfo_commlname_taxidentity(root, prev_df):
    included = ['TaxIdentity', 'TaxIdTypeCd', 'TaxId']
    rows = []
    for el in root.findall('NameInfo'):
        row = {}
        for element in el.iter():
            if element.tag not in included: continue
            row.update(process_element(element))
        if not row: continue
        row.update(add_fk(['PersAutoPolicyModRq_Producer', 'PersAutoPolicyModRq', 'Producer_GeneralPartyInfo'], prev_df))
        row['FK_GeneralPartyInfo_NameInfo'] = generate_id(el, row['FK_Producer_GeneralPartyInfo'])
        row['PK_NameInfo_TaxIdentity'] = generate_id(el, row['FK_GeneralPartyInfo_NameInfo'])
        rows.append(row)
    return pd.DataFrame(rows)


if __name__ == '__main__':
    
    print(pd.__version__)
    tree = ET.parse(os.path.join(os.getcwd(), INPUT_FILE))
    root = tree.getroot()
    
    try:
        main_df = persautopolicymodrq(root)
        producer_df = persautopolicymodrq_producer(root, main_df)
        itemidinfo_df = persautopolicymodrq_producer_itemidinfo(root.find('Producer'), producer_df)
        generalpartyinfo_df = persautopolicymodrq_producer_generalpartyinfo(root.find('Producer'), itemidinfo_df)
        producerinfo_df = persautopolicymodrq_producer_producerinfo(root.find('Producer'), generalpartyinfo_df)
        nameinfo_df = persautopolicymodrq_producer_generalpartyinfo_nameinfo(root.find('Producer').find('GeneralPartyInfo'), generalpartyinfo_df)
        commlname_df = persautopolicymodrq_producer_generalpartyinfo_nameinfo_commlname(root.find('Producer').find('GeneralPartyInfo'), nameinfo_df)
        taxidentity_df = persautopolicymodrq_producer_generalpartyinfo_nameinfo_commlname_taxidentity(root.find('Producer').find('GeneralPartyInfo'), commlname_df)
    except Exception as e:
        logger.error(f'Error processing {INPUT_FILE}: {e}')
        raise
    
    # save
    save(main_df, 'TABLE_PersAutoPolicyModRq')
    save(producer_df, 'TABLE_PersAutoPolicyModRq_Producer')
    save(itemidinfo_df, 'TABLE_PersAutoPolicyModRq_Producer_ItemIdInfo')
    save(generalpartyinfo_df, 'TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo')
    save(producerinfo_df, 'TABLE_PersAutoPolicyModRq_Producer_ProducerInfo')
    save(nameinfo_df, 'TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo')
    save(commlname_df, 'TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo_CommlName')
    save(taxidentity_df, 'TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo_CommlName_TaxIdentity')
    