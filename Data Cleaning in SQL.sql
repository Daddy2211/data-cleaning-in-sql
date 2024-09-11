-- DATA CLEANING

SELECT *
FROM layoffs;

-- Remove Duplicates
-- Standardize The Data
-- Null Values or Blank Values
-- Remove Any Columns

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
row_number() over(
PARTITION BY company, industry, total_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte as
(
SELECT *,
row_number() over(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

SELECT *
FROM layoffs_staging
where company = 'Casper';

SELECT *,
row_number() over(
PARTITION BY company, industry, total_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte as
(
SELECT *,
row_number() over(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
where row_num > 1;

insert into layoffs_staging2
SELECT *,
row_number() over(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;

-- Standardizing Data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE ;

select *
from layoffs_staging2
where total_laid_off is NULL
AND percentage_laid_off is NULL;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is NULL
or industry = '';

select *
from layoffs_staging2
where company like 'Bally%';

select t1.industry, t2.industry
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
where (t1.industry IS null or t1.industry = '')
AND t2.industry is not null;

update layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
set t1.industry = t2.industry
where t1.industry IS null
AND t2.industry is not null;

select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is NULL
AND percentage_laid_off is NULL;

DELETE
from layoffs_staging2
where total_laid_off is NULL
AND percentage_laid_off is NULL;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;



















































































































