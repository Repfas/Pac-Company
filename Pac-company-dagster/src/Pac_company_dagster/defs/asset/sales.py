from dagster import asset, OpExecutionContext



# asset for EXTRACT
@asset(group_name="sales") 
def extract_sales_raw(context: OpExecutionContext):
    '''Asset: Extraction raw data from csv'''
    context.log.info("Extracting sales data from csv...")
    # call the sales extract 
    from Pac_company_dagster.pipeline.extract.extract_sales import extract_sales
    raw_sales_path = extract_sales()
    context.log.info(f"sales team data has been extracted")
    return raw_sales_path

@asset(group_name= 'sales')
def transform_sales(context: OpExecutionContext, extract_sales_raw):
    '''Asset: Transforming raw data into clean data'''
    context.log.info('Transforming raw sales data')

    # call the function manipulation sales 
    from Pac_company_dagster.pipeline.transform.sales.manipulation_sales import sales_manipulation
    clean_data_path = sales_manipulation(extract_sales_raw)
    context.log.info('Data sales cleaned')
    return clean_data_path

@asset(group_name = 'sales')
def create_dim_product_sales(context: OpExecutionContext, transform_sales):
    '''Asset: Transforming clean_data into dim_product'''
    context.log.info('Transforming clean_data into dim_product')

    # call the function manipulation dim_product
    from Pac_company_dagster.pipeline.transform.sales.manipulation_dim_product_sales import manipulation_dim_product_sales
    dim_manipulation_path = manipulation_dim_product_sales(transform_sales)
    context.log.info('dim_poduct created')
    return dim_manipulation_path

@asset(group_name = 'sales')
def create_fct_sales_price(context: OpExecutionContext, 
                                    transform_sales,
                                    create_dim_product_sales):
    '''Asset: Transforming clean_data into fct_sales_transaction.'''
    context.log.info('Transforming clean_data into fct_sales_transaction.')

    # call the function manipulation fct_product_price
    from Pac_company_dagster.pipeline.transform.sales.manipulation_fct_sales import manipulation_fct_sales
    fct_path = manipulation_fct_sales(transform_sales,
                                    create_dim_product_sales)
    context.log.info('dim_poduct_category created')
    return fct_path

@asset(group_name= 'sales')
def load_dim_product_sales_clickhouse(context: OpExecutionContext,create_dim_product_sales):
    '''Load dim product into sales_dwh '''
    context.log.info('Loading dim_product intosales dwh')

    from Pac_company_dagster.pipeline.load.load_dim_product_sales import load_dim_product_sales 
    load = load_dim_product_sales(create_dim_product_sales)
    context.log.info('dim_product has been load successfully')
    return load

@asset(group_name= 'sales')
def load_fct_sales_transaction_clickhouse(context: OpExecutionContext,create_fct_sales_price):
    '''Load fct product price into product_dwh '''
    context.log.info('Loading fct_product_price into product dwh')

    from Pac_company_dagster.pipeline.load.load_fct_sales_transaction import load_fct_sales_transaction
    load = load_fct_sales_transaction(create_fct_sales_price)
    context.log.info('fct_sales transaction has been load successfully')
    return load
