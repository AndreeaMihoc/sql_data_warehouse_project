use [DataWarehouse]

insert into silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)

select  
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case  when upper(trim(cst_material_status)) = 'S' then 'Single'
      when upper(trim(cst_material_status)) = 'M' then 'Married'
      else 'n/a'
end cst_marital_status,
case  when upper(trim(cst_gndr)) = 'F' then 'Female'
      when upper(trim(cst_gndr)) = 'M' then 'Male'
      else 'n/a'
end cst_gndr,
cst_create_date
from(
select
*, 
row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from [bronze].[crm_cust_info] where cst_id is not null) as t 
where cst_id =1;






SELECT [prd_id],
      [prd_key],
      replace(SUBSTRING(prd_key, 1, 5), '-','_') as cat_id,
      SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
      [prd_nm],
      isnull([prd_cost],0) as prd_cost,
      case 
        when upper(trim(prd_line)) = 'M' then 'Mountain'
        when UPPER(trim(prd_line)) = 'S' then 'other Sales'
        when UPPER(trim(prd_line)) = 'R' then 'Road'
        when UPPER(trim(prd_line)) = 'T' then 'Touring'
        else 'n/a'
end as prd_line,
      cast([prd_start_dt] as date) as prd_start_dt,
      cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt
  FROM [DataWarehouse].[bronze].[crm_prd_info];



