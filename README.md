# Database Query (DBQ)
This package can be used to query on aerospike or elastic search using ORM type queries.

#### Example
``` python
    
    # Aerospike
    from dbq import ASClient
    as_client = ASClient(hosts=['127.0.0.1', 3000])
    dsi = as_client.get_model(namespace='optimus', set='arch_dsi')

    # Elastic Search
    from dbq import ESClient
    client = ESClient(hosts=['10.5.245.49'])
    dsi = client.get_model(index='optimus_si')

```

## 1. Querying Model
You can either do full scan or scan for specific primary keys and fetch just the required records based on the filters

## 1.1 filter/exclude options
    - iexact: Case insensitive exact string match
    - contains: Case sensitive substring match
    - icontains: Case insensitive substring match
    - in: bin value is in list
    - gt: bin value is greater than passed value 
    - gte: bin value is greater than or equal to passed value
    - lt: bin value is less than passed value 
    - lte: bin value is less than or equal to passed value
    - ne: bin value is not equal to passed value
    - nested queries: field1__field2__filter
    - custom filter: field = custom_filter_fn, custom_filter receives dict of bins and should return a boolean value

## 1.2 select
list of bins to fetch, for nested bins use '__' eg: field1__field2

## 1.3 scan
You can use it in cases where you don't have the information of primary keys and want to full scan

#### 1.3.1 scan attributes
    - max_records_count (int): Number of valid records to fetch, -1 for full scan
    - max_scans_count (int): Number of records to scan, -1 for full scan
    - save_file (string): file path to save results
    - save_format (string): either 'csv' or 'json', default is json

`- If max_scans_count > 100000 or -1 save_file is required`
`- In case of csv format select attribute is required`

#### 1.3.2 Examples
``` python
    data = dsi.objects\
        .filter(res_addrss__pincode__in=['700107', '122003'])\
        .select('res_addrss__pincode')\
        .scan(5)

    data = dsi.objects\
        .filter(res_addrss__pincode=lambda x: x < '150000',
                res_addrss__pincode__in=['122001'])\
        .select('res_addrss__pincode')\
        .scan(5)

    data = dsi.objects\
        .exclude(si_is_btfly__in=['1', None])\
        .select('si_is_btfly', 'si')\
        .scan(100)

    data = dsi.objects\
        .exclude(cust_cat='M2M', cust_seg='Gold')\
        .select('cust_cat', 'cust_seg')\
        .scan(10)

    data = dsi.objects\
        .filter(si_prod_type='FLVOICE')\
        .select('pk', si_prod_type')\
        .scan(-1, save_file='output.json', save_format='csv')

    data = dsi.objects\
        .filter(si_prod_type__iexact='FLVOICE')\
        .select('si_prod_type')\
        .scan(1)

    data = dsi.objects.\
        filter(si_prod_type__icontains='VoIC')\
        .select('si_prod_type')\
        .scan(1)
```

## 1.3 get
If you have the list of pks you can use get function

### 1.3.1 get attributes
    - pks (list): list of primary keys
    - pk_file: file containing primary keys separated with comma or newline
    - save_file (string): file path to save results
    - save_format (string): either 'csv' or 'json', default is json

`- Either of pks or input_file is required`
`- In case of csv format select attribute is required`

### 1.3.2 Example
```python
    data = dsi.objects\
        .filter(record_ind=None)\
        .select('record_ind')\
        .get(pks=['03340651199'])

    data = dsi.objects\
        .filter(record_ind=None)\
        .select('si', record_ind')\
        .get(
            pk_file='abc/input_file.txt',
            save_file='output.csv',
            save_format='csv'
        )
```

## 1.4 Date filter
For filtering out results basis on date, accepted formats 'yyyy-mm-dd' or 'yyyy-mm-dd HH:MM:SS'

### 1.4.1 Examples
```python
    from dbq import ASClient, date_filter

    client = ASClient(hosts=['127.0.0.1', 3000])
    dsi = client.get_model(namespace='optimus', set='arch_dsi')
    
    data = dsi.objects\
        .filter(crt_dttm__gt=date_filter('2018-10-24'),
                crt_dttm__lt=date_filter('2018-10-26 16:10:10'))\
        .select('si', 'crt_dttm')
        .scan(1)
```
