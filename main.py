import os
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark import StorageLevel
from pyspark.errors.exceptions.captured import AnalysisException


# logging conf
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# constants
OUTPUT_BASE_PATH = 'output'
INPUT_FILE = 'sample/data.xml'

# utils
f.udf(returnType=t.StringType())
def get_id(row):
    """ Generate a hash based on df columns to use it as PK """
    return f.sha1(f.to_json(row))

def save_and_cleanup(df, table_name):
    """ Save df to parquet and clean memory """
    target_path = os.path.join(OUTPUT_BASE_PATH, table_name)
    try:
        df.repartition(200).write.format('parquet') \
            .option('compression', 'snappy') \
            .mode('overwrite') \
            .save(target_path)
        df.unpersist()
        logger.info(f'Saved {table_name} to {target_path}')
    except Exception as e:  # Catch any, TODO: handle specific exceptions
        logger.error(f'Error saving {table_name}: {e}')
        raise
    
def persautopolicymodrq(df):
    # TABLE_PersAutoPolicyModRq
    return  df.select(
        get_id(f.struct(
            f.col('TransactionEffectiveDt'),
            f.col('TransactionRequestDt'),
            f.col('RequId'),
            f.col('Producer')
        )).alias('PK_PersAutoPolicyModRq'),
        f.col('RequId'),
        f.col('TransactionRequestDt._value').alias('TransactionRequestDt'),
        f.col('TransactionRequestDt._id').alias('TransactionRequestDt_id'),
        f.col('TransactionEffectiveDt._value').alias('TransactionEffectiveDt'),
        f.col('TransactionEffectiveDt._id').alias('TransactioneffectiveDt_id'),
        f.col('Producer')
    ).persist(StorageLevel.MEMORY_AND_DISK)

def persautopolicymodrq_producer(df):
    return df.select(
        f.col('PK_PersAutoPolicyModRq').alias('FK_PersAutoPolicyModRq'),
        get_id(f.struct(
            f.col('PK_PersAutoPolicyModRq'),
            f.col('Producer')
        )).alias('PK_PersAutoPolicyModRq_Producer'),
        f.col('Producer._id').alias('Proucer_id')
    ).persist(StorageLevel.MEMORY_AND_DISK)

def persautopolicymodrq_producer_itemidinfo(df, main_df):
    return df.join(
        f.broadcast(main_df), 
        main_df['PK_PersAutoPolicyModRq'] == df['FK_PersAutoPolicyModRq']
    ).select(
        f.col('PK_PersAutoPolicyModRq').alias('FK_PersAutoPolicyModRq'),
        f.col('PK_PersAutoPolicyModRq_Producer').alias('FK_PersAutoPolicyModRq_Producer'),
        get_id(f.struct(
            f.col('PK_PersAutoPolicyModRq_Producer'),
            f.col('Producer.ItemIdInfo')
        )).alias('PK_PersAutoPolicyModRq_Producer_ItemIdInfo'),
        f.col('Producer.ItemIdInfo._id').alias('ItemIdInfo_id'),
        f.col('Producer.ItemIdInfo.AgencyId._value').cast('string').alias('AgencyId'),
        f.col('Producer.ItemIdInfo.AgencyId._id').alias('AgencyId_id'),
        f.col('Producer.ItemIdInfo.InsurerId._value').alias('InsurerId'),
        f.col('Producer.ItemIdInfo.InsurerId._id').alias('InsurerId_id')
    ).persist(StorageLevel.MEMORY_AND_DISK)

def persautopolicymodrq_producer_generalpartyinfo(df, main_df):
    return df.join(
        f.broadcast(main_df), 
        main_df['PK_PersAutoPolicyModRq'] == df['FK_PersAutoPolicyModRq']
    ).select(
        f.col('FK_PersAutoPolicyModRq'),
        f.col('FK_PersAutoPolicyModRq_Producer'),
        get_id(f.struct(
            f.col('FK_PersAutoPolicyModRq_Producer'),
            f.col('Producer.GeneralPartyInfo')
        )).alias('PK_Producer_GeneralPartyInfo'),
        f.col('Producer.GeneralPartyInfo._id').alias('GeneralPartyInfo_id')
    ).persist(StorageLevel.MEMORY_AND_DISK)

def persautopolicymodrq_producer_generalpartyinfo_nameinfo(df, main_df):
    return df.join(
        f.broadcast(main_df),
        main_df['PK_PersAutoPolicyModRq'] == df['FK_PersAutoPolicyModRq']
    ).select(
        f.col('FK_PersAutoPolicyModRq'),
        f.col('FK_PersAutoPolicyModRq_Producer'),
        f.col('PK_Producer_GeneralPartyInfo').alias('FK_Producer_GeneralPartyInfo'),
        f.col('Producer.GeneralPartyInfo.NameInfo'),
        f.explode('Producer.GeneralPartyInfo.NameInfo._id').alias('NameInfo_id')
    ).withColumn(
        'PK_GeneralPartyInfo_NameInfo',
        get_id(f.struct(
            f.col('FK_PersAutoPolicyModRq'),
            f.col('NameInfo_id'),
            f.col('NameInfo')
        ))
    ).persist(StorageLevel.MEMORY_AND_DISK)

def persautopolicymodrq_producer_generalpartyinfo_nameinfo_commlname(df):
    _pivot = df.select(
        f.col('PK_GeneralPartyInfo_NameInfo').alias('_PK_GeneralPartyInfo_NameInfo'),
        f.explode('NameInfo').alias('_NameInfo')
    )
    
    _pivot = df.join(
        f.broadcast(_pivot), 
        [(_pivot['_PK_GeneralPartyInfo_NameInfo'] == df['PK_GeneralPartyInfo_NameInfo']) & (_pivot['_NameInfo._id'] == generalpartyinfo_nameinfo_df['NameInfo_id'])]
    ).select(
        f.col('PK_GeneralPartyInfo_NameInfo'),
        f.col('_NameInfo.CommlName._id').alias('CommlName_id'),
        f.col('_NameInfo.CommlName.CommercialName._id').alias('CommercialName_id'),
        f.col('_NameInfo.CommlName.CommercialName._value').alias('CommercialName')
    )
    _pivot.unpersist()
    
    return df.select(
        f.col('FK_PersAutoPolicyModRq'),
        f.col('FK_PersAutoPolicyModRq_Producer'),
        f.col('FK_Producer_GeneralPartyInfo'),
        f.col('PK_GeneralPartyInfo_NameInfo'),
        f.col('NameInfo')
    ).join(
        f.broadcast(_pivot),
        'PK_GeneralPartyInfo_NameInfo'
    ).select(
        f.col('FK_PersAutoPolicyModRq'),
        f.col('FK_PersAutoPolicyModRq_Producer'),
        f.col('FK_Producer_GeneralPartyInfo'),
        f.col('PK_GeneralPartyInfo_NameInfo').alias('FK_GeneralPartyInfo_NameInfo'),
        get_id(f.struct(
            f.col('FK_GeneralPartyInfo_NameInfo'),
            f.col('CommlName_id'),
            f.col('NameInfo.CommlName')
        )).alias('PK_NameInfo_CommlName'),
        f.col('CommlName_id'),
        f.col('CommercialName'),
        f.col('CommercialName_id')
    ).persist(StorageLevel.MEMORY_AND_DISK)

def persautopolicymodrq_producer_generalpartyinfo_nameinfo_commlname_taxidentity(df):
    taxidentity_pivot_df = df.select(
        f.col('PK_GeneralPartyInfo_NameInfo'),
        f.explode('NameInfo._id').alias('_NameInfo_id'),
        f.explode('NameInfo.TaxIdentity').alias('TaxIdentity')
    ).filter(f.col('TaxIdentity').isNotNull()).drop_duplicates(['_NameInfo_id'])
    
    return df.join(
        f.broadcast(taxidentity_pivot_df),
        [(taxidentity_pivot_df['PK_GeneralPartyInfo_NameInfo'] == df['PK_GeneralPartyInfo_NameInfo']) & (taxidentity_pivot_df['_NameInfo_id'] == generalpartyinfo_nameinfo_df['NameInfo_id'])],
        'inner'
    ).select(
        f.col('FK_PersAutoPolicyModRq'),
        f.col('FK_PersAutoPolicyModRq_Producer'),
        f.col('FK_Producer_GeneralPartyInfo'),
        taxidentity_pivot_df['PK_GeneralPartyInfo_NameInfo'].alias('FK_GeneralPartyInfo_NameInfo'),
        get_id(f.struct(
            f.col('FK_GeneralPartyInfo_NameInfo'),
            f.col('TaxIdentity._id'),
            f.col('NameInfo.TaxIdentity')
        )).alias('PK_NameInfo_TaxIdentity'),
        f.col('TaxIdentity._id').alias('TaxIdentity_id'),
        f.col('TaxIdentity.TaxIdTypeCd._value').alias('TaxIdTypeCd'),
        f.col('TaxIdentity.TaxIdTypeCd._id').alias('TaxIdTypeCd_id'),
        f.col('TaxIdentity.TaxId._value').alias('TaxId'),
        f.col('TaxIdentity.TaxId._id').alias('TaxId_id')
    ).persist(StorageLevel.MEMORY_AND_DISK)

def persautopolicymodrq_producer_producerinfo(df, main_df):
    return df.join(
        f.broadcast(main_df),
        df['FK_PersAutoPolicyModRq'] == main_df['PK_PersAutoPolicyModRq']
    ).select(
        f.col('FK_PersAutoPolicyModRq'),
        f.col('PK_PersAutoPolicyModRq_Producer').alias('FK_PersAutoPolicyModRq_Producer'),
        f.explode('Producer.ProducerInfo').alias('ProducerInfo')
    ).withColumn(
        'PK_Producer_ProducerInfo',
        get_id(f.struct(
            f.col('FK_PersAutoPolicyModRq_Producer'),
            f.col('ProducerInfo._id'),
            f.col('ProducerInfo')
        ))
    ).withColumn(
        'ProducerInfo_id', f.col('ProducerInfo._id')
    ).withColumn(
        'ContractNumber', f.col('ProducerInfo.ContractNumber._value')
    ).withColumn(
        'ContractNumber_id', f.col('ProducerInfo.ContractNumber._id')
    ).withColumn(
        'ProducerSubCode', f.col('ProducerInfo.ProducerSubCode._value')
    ).withColumn(
        'ProducerSubCode_id', f.col('ProducerInfo.ProducerSubCode._id')
    ).persist(StorageLevel.MEMORY_AND_DISK)

if __name__ == '__main__':
    
    with SparkSession.builder.appName('Indra Test').getOrCreate() as spark:
        input_df = spark.read.format('com.databricks.spark.xml').options(rowTag='PersAutoPolicyModRq').load(os.path.join(os.getcwd(), INPUT_FILE))
        
        try:
            # TABLE_PersAutoPolicyModRq
            main_df = persautopolicymodrq(input_df)
            
            # TABLE_PersAutoPolicyModRq_Producer
            producer_df = persautopolicymodrq_producer(main_df)
            
            # TABLE_PersAutoPolicyModRq_Producer_ItemIdInfo
            producer_itemidinfo_df = persautopolicymodrq_producer_itemidinfo(producer_df, main_df)
            
            # TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo
            producer_generalpartyinfo_df = persautopolicymodrq_producer_generalpartyinfo(producer_itemidinfo_df, main_df)
            
            # TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo
            generalpartyinfo_nameinfo_df = persautopolicymodrq_producer_generalpartyinfo_nameinfo(producer_generalpartyinfo_df, main_df)

            # TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo_CommlName
            nameinfo_commlname_df = persautopolicymodrq_producer_generalpartyinfo_nameinfo_commlname(generalpartyinfo_nameinfo_df)
            
            # TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo_CommlName_TaxIdentity      
            taxidentity_df = persautopolicymodrq_producer_generalpartyinfo_nameinfo_commlname_taxidentity(generalpartyinfo_nameinfo_df)
            
            # TABLE_PersAutoPolicyModRq_Producer_ProducerInfo
            producer_producerinfo_df = persautopolicymodrq_producer_producerinfo(producer_df, main_df)
        except Exception as e:  # Catch any, TODO: handle specific exceptions
            logger.error(f'Error processing data: {e}')
            raise
        
        # save I/O
        save_and_cleanup(main_df.drop('Producer'), 'TABLE_PersAutoPolicyModRq')
        save_and_cleanup(producer_df, 'TABLE_PersAutoPolicyModRq_Producer')
        save_and_cleanup(producer_itemidinfo_df, 'TABLE_PersAutoPolicyModRq_Producer_ItemIdInfo')
        save_and_cleanup(producer_generalpartyinfo_df, 'TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo')
        save_and_cleanup(nameinfo_commlname_df, 'TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo_CommlName')
        save_and_cleanup(generalpartyinfo_nameinfo_df.drop('NameInfo'), 'TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo')
        save_and_cleanup(taxidentity_df, 'TABLE_PersAutoPolicyModRq_Producer_GeneralPartyInfo_NameInfo_CommlName_TaxIdentity')
        save_and_cleanup(producer_producerinfo_df.drop('ProducerInfo'), 'TABLE_PersAutoPolicyModRq_Producer_ProducerInfo')