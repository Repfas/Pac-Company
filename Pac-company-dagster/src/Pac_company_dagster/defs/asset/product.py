from dagster import asset, OpExecutionContext
import clickhouse_connect 



# asset for EXTRACT
@asset(group_name="product") 
def extract_product_raw(context: OpExecutionContext):
    '''Asset: Extraction raw data from csv'''
    context.log.info("Extracting product data from csv...")

    # call function extract_product.py 
    from Pac_company_dagster.pipeline.extract.extract_product import extract_product
    raw_product_path = extract_product()
    context.log.info(f"Extracted")
    return raw_product_path


@asset(group_name= 'product')
def transform_product(context: OpExecutionContext, extract_product_raw):
    '''Asset: Transforming raw data into clean data'''
    context.log.info('Transforming raw product data')

    # call the function manipulation product 
    from Pac_company_dagster.pipeline.transform.product.manipulation_product import product_manipulation
    clean_data_path = product_manipulation(extract_product_raw)
    context.log.info('Data Product cleaned')
    return clean_data_path


@asset(group_name = 'product')
def create_dim_product(context: OpExecutionContext, transform_product):
    '''Asset: Transforming clean_data into dim_product'''
    context.log.info('Transforming clean_data into dim_product')

    # call the function manipulation dim_product
    from Pac_company_dagster.pipeline.transform.product.manipulation_dim_product import manipulation_dim_product
    dim_manipulation_path = manipulation_dim_product(transform_product)
    context.log.info('dim_poduct created')
    return dim_manipulation_path

@asset(group_name = 'product')
def create_dim_date(context: OpExecutionContext):
    '''Asset: generate dim_date'''
    context.log.info('Generating dim_date')

    # call the function manipulation dim_product
    from Pac_company_dagster.pipeline.transform.product.manipulation_dim_date import generate_dim_date
    dim_manipulation_path = generate_dim_date()
    context.log.info('dim date created')
    return dim_manipulation_path

@asset(group_name = 'product')
def create_dim_merchant(context: OpExecutionContext, transform_product):
    '''Asset: Transforming clean_data into dim_merchant'''
    context.log.info('Transforming clean_data into dim_merchant')

    # call the function manipulation dim_merchant
    from Pac_company_dagster.pipeline.transform.product.manipulation_dim_merchant import manipulation_dim_merchant
    dim_manipulation_path = manipulation_dim_merchant(transform_product)
    context.log.info('dim_merchant created')
    return dim_manipulation_path

@asset(group_name = 'product')
def create_dim_product_category(context: OpExecutionContext, transform_product):
    '''Asset: Transforming clean_data into dim_product_category'''
    context.log.info('Transforming clean_data into dim_product_category')

    # call the function manipulation dim_product_category
    from Pac_company_dagster.pipeline.transform.product.manipulation_dim_product_category import manipulation_dim_product_category
    dim_manipulation_path = manipulation_dim_product_category(transform_product)
    context.log.info('dim_poduct_category created')
    return dim_manipulation_path

@asset(group_name = 'product')
def create_fct_product_price(context: OpExecutionContext, 
                                    transform_product,
                                    create_dim_product,
                                    create_dim_date,
                                    create_dim_merchant,
                                    create_dim_product_category):
    '''Asset: Transforming clean_data into fct_product_price'''
    context.log.info('Transforming clean_data into fct_product_price')

    # call the function manipulation fct_product_price
    from Pac_company_dagster.pipeline.transform.product.manipulation_fct_product import manipulation_fct_product
    fct_path = manipulation_fct_product(transform_product,create_dim_product,create_dim_date,
                                                     create_dim_merchant,create_dim_product_category)
    context.log.info('dim_poduct_category created')
    return fct_path


@asset(group_name= 'product')
def load_dim_product_clickhouse(context: OpExecutionContext,create_dim_product):
    '''Load dim product into product_dwh '''
    context.log.info('Loading dim_product into product dwh')

    from Pac_company_dagster.pipeline.load.load_dim_product import load_dim_product 
    load = load_dim_product(create_dim_product)
    context.log.info('dim_product has been load successfully')
    return load

@asset(group_name= 'product')
def load_dim_product_category_clickhouse(context: OpExecutionContext,create_dim_product_category):
    '''Load dim product category into product_dwh '''
    context.log.info('Loading dim_product_category into product dwh')

    from Pac_company_dagster.pipeline.load.load_dim_product_category import load_dim_product_category 
    load = load_dim_product_category(create_dim_product_category)
    context.log.info('dim_product_category has been load successfully')
    return load
@asset(group_name= 'product')
def load_dim_date_clickhouse(context: OpExecutionContext,create_dim_date):
    '''Load dim date into product_dwh '''
    context.log.info('Loading dim_date into product dwh')

    from Pac_company_dagster.pipeline.load.load_dim_date import load_dim_date 
    load = load_dim_date(create_dim_date)
    context.log.info('dim_date has been load successfully')
    return load

@asset(group_name= 'product')
def load_dim_merchant_clickhouse(context: OpExecutionContext,create_dim_merchant):
    '''Load dim merchant into product_dwh '''
    context.log.info('Loading dim_merchant into product dwh')

    from Pac_company_dagster.pipeline.load.load_dim_merchant import load_dim_merchant 
    load = load_dim_merchant(create_dim_merchant)
    context.log.info('dim_merchant has been load successfully')
    return load

@asset(group_name= 'product')
def load_fct_product_price_clickhouse(context: OpExecutionContext,create_fct_product_price):
    '''Load fct product price into product_dwh '''
    context.log.info('Loading fct_product_price into product dwh')

    from Pac_company_dagster.pipeline.load.load_fct_product_price import load_fct_product_price
    load = load_fct_product_price(create_fct_product_price)
    context.log.info('fct_product has been load successfully')
    return load