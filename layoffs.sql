USE alex_layoffs;
SELECT 
    *
FROM
    layoffs;

--- crate a copy of table so that we can use it for data cleaning and leave the original one as it is

CREATE TABLE layoff_staging LIKE layoffs;

SELECT 
    *
FROM
    layoff_staging;

insert layoff_staging 
select * from layoffs;

--- Data Cleaning 
 --- 1. Remove duplicates
 --- 2. standardize data
 --- 3. Look for null values 
 --- 4. remove rows and columns that are not necessary
 
 --- Remove duplicates
 
 select company, industry, total_laid_off, date,
 row_number() over(partition by company, industry, total_laid_off, date) as row_num
 from layoff_staging;
 
 select * 
 from (select *,
 row_number() over(partition by company, industry, total_laid_off, date) as row_num
 from layoff_staging) as duplicates
 where row_num >1;
 
select * 
 from (select *,
 row_number() over(partition by company, location, industry, total_laid_off, date, percentage_laid_off, stage, country, funds_raised_millions) as row_num
 from layoff_staging) as duplicates
 where row_num >1;
 
 --- Add row_num to the table. 
 ------------------------Didnt  worked-------------------------------
 alter table layoff_staging
 add column row_numb int;
 
SELECT 
    *
FROM
    layoff_staging;
 
 insert into layoff_staging( company, location, industry, total_laid_off, date, percentage_laid_off, stage, country, funds_raised_millions, row_numb)
 select  company, location, industry, total_laid_off, date, percentage_laid_off, stage, country, funds_raised_millions,row_number() over(partition by company, location, industry, total_laid_off, date, percentage_laid_off, stage, country, funds_raised_millions) as row_num
 from layoff_staging;
 
delete FROM layoff_staging 
WHERE
    row_numb IS NOT NULL;
    
alter table layoff_staging
drop column row_numb;

-- --------------------Didnt worked---------------------
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,date, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,date, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoff_staging
) duplicates
WHERE 
	row_num > 1
)
select *
FROM DELETE_CTE
;
------------------------------------------------------------------------------
CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoff_staging2;



insert into layoff_staging2
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,date, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoff_staging;
        
        
 delete
from layoff_staging2
where row_num > 1;       

-- Standardization-----------------------------

Select distinct company,trim(company) from layoff_staging2;

update layoff_staging2
set company = trim(company);

select distinct industry 
from layoff_staging2
where industry is not null
order by 1 ;

update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country 
from layoff_staging2
order by 1;

Update layoff_staging2
set country = trim(Trailing '.' from country);

-- Null Values-----------------------------

Select * from layoff_staging2  
where   industry is null or industry = '' 
order by company;

update layoff_staging2
set industry = null
where industry = '';

select * 
from layoff_staging2
where company like 'Airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values


update layoff_staging2  t1
join layoff_staging2  t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null   and  t2.industry is not null;

update layoff_staging2
set date = str_to_date(date,'%m/%d/%Y');

 select date from layoff_staging2;
 
 alter table layoff_staging2
 modify column date date;
 
 -- remove column and rows----------------------
 select * from layoff_staging2;
 
 alter table layoff_staging2
 drop row_num;
 
 select * from layoff_staging2;