select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;
-- thats a lof people who were laid off 
-- lets  see how many companies 

select*
from layoffs_staging2
where percentage_laid_off = 1;

-- lets see what company had the largest amount of total_laid_off

select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

-- lets look at companies that had a lot of funding but went under  

select*
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;


select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;

-- the dates are showing the timeline of all these layoffs and during that peroid there was covid and i think its a huge reason for all these layoffs
-- lets take a look at the industry 

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- the industries that were hit really hard were consumer , retail, other , transportation,finance and healthcare in that order ,
-- its makes sense considering the peroid all these is happening 
-- covid still stands out as a huge factor .
-- as we are looking at the high numbers of layoffs its also important to look at the smaller numbers too 
-- the bottom 5 from legal, energy, aerospace, fin-tech, and manufacturing kind of make sesne why they had lesser layoffs 
-- my assumption is leaning heavily on the effect of corona 
-- lets take a look at the countries now 

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- lets look at the years 

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

-- i think my asumption was wrong ,
-- looking at the years with hte most layoffs the year 2020 has the least amount of layoffs
-- its shocking to learn that the numbers are more in the year 2023 and the data i'm using if from the same year three months in
-- lets take  a look at the stage this companies were at and compare it to the layoffs 


select	stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

-- looks like most layoffs come from the companies that are in the late stages 
-- mostly the post-ipo 

select	company, sum(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

-- lets do a rolling total

with rolling_total as
 (
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off
,sum(total_off) over(order by `month` ) as rolling_total
from rolling_total;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by company asc;

with company_year(company,years,total_laid_off)as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)

), company_year_rank as 
(select*,
dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking  >= 5
;