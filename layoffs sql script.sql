select *
from layoffs;
-- data cleaning 
-- first step,remove duplicates
-- second is to standardize the data
-- null or blank values
-- remove any unnecessary columns
-- but first , i'll stage the data to avoid using the raw data
 
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging 
select*
from layoffs;

select*,
row_number() over(
partition by  company,industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_staging;

with duplicate_cte as 
(
select*,
row_number() over(
partition by  company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
select*
from duplicate_cte
where row_num> 1;
-- oda ins't on there 
select*
from layoffs_staging
where company = 'casper';

select*
from layoffs_staging
where company='oda';
-- they are not duplicates as i thought they were so i'll check for more to see if there are duplicates
-- i will now do the partition by every single column

with duplicate_cte as 
(
select*,
row_number() over(
partition by  company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
delete 
from duplicate_cte
where row_num> 1;

create table `layoffs_staging2` (
`company` text,
`location` text,
`industry` text,
`total_laid_off` int DEFAULT NULL,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int default null,
`row_num` int
) engine=innodb default charset=utf8mb4 collate=utf8mb4_0900_ai_ci;

select*
from layoffs_staging2;
 
insert into layoffs_staging2
select*,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) as row_num
from layoffs_staging;

select* 
from layoffs_staging2
where row_num > 1;
 
delete 
from layoffs_staging2
where row_num > 1;

SET SQL_SAFE_UPDATES = 0;

delete 
from layoffs_staging2
where row_num > 1;

select* 
from layoffs_staging2
where row_num > 1;

select*
from layoffs_staging2;
-- duplicates are removed now
-- now lets standardize the data

select distinct(trim(company))
from layoffs_staging2; 

update layoffs_staging2
set company = trim(company);

select*
from layoffs_staging2;

select distinct industry
from layoffs_staging2
order by 1;
select*
from layoffs_staging2
where industry like'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select*
from layoffs_staging2
where industry like'crypto%';


select distinct industry
from layoffs_staging2;

select distinct country
from layoffs_staging2
order by 1;    


select 'date'
from layoffs_staging2;
where country like 'united states%'
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;  

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united states%';


select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');
select`date`
from layoffs_staging2;


alter table layoffs_staging2
modify column `date` date;

-- now i have to deal with null values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select distinct industry
from layoffs_staging2;

-- theres a missing value and a null

select *
from layoffs_staging2
where industry is null
or industry = '';
-- we'll try to populate data . start with air bnb

select * 
from layoffs_staging2
where company = 'airbnb';
-- air bnb is under the travel industry so the null value isn't something we should leave blank
-- also we'll do the same for the rest of the null or blanks in the industry column


select * 
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where( t1.industry is  null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = null 
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
 set t1.industry = t2.industry
 where t1.industry is null 
 and t2.industry is not null;
 


select* 
from layoffs_staging2
where company like 'bally%';

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
-- there are several companies that have null values in both the total laid off column and percentage laid off ,
-- deleting this data will be okay since we cant work with the data if the values are null

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


alter table layoffs_staging2
drop column row_num;


select *
from layoffs_staging2;

-- to this point the data is clean, standardized and usable 
-- next thing is exploratory data analysis on this data and run more complex scripts 

