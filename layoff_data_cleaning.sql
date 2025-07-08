-- Cleaning of Layoff dataset across all industries -----
SELECT * FROM layoffs;

-- STEPS: --
-- Remove duplicate --
-- Standardize --
-- Handle Nulls and Blanks --
-- Remove unneeded columns and rows --

-- Creating a staging table --
-- It's a best practice not to work directly on raw data --
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- Remove duplicate --
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as Row_Num
FROM layoffs_staging;

WITH layoffs_staging_row AS
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as Row_Num
FROM layoffs_staging)
SELECT *
FROM layoffs_staging_row
WHERE Row_Num > 1;

WITH layoffs_staging_row AS
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as Row_Num
FROM layoffs_staging)
SELECT *
FROM layoffs_staging_row
WHERE Row_Num > 1;

UPDATE layoffs_staging
SET Row_Num = ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions);

-- Since Mysql doesn't support update at cte level, we need to create another staging table --

CREATE TABLE `layoffs_staging_1` (
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

SELECT *
FROM layoffs_staging_1;


INSERT layoffs_staging_1
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
FROM layoffs_staging;

SELECT *
FROM layoffs_staging_1;

DELETE 
FROM layoffs_staging_1
WHERE row_num > 1;

-- Standardize --

SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging_1;

UPDATE layoffs_staging_1
SET company = TRIM(company);

SELECT industry
FROM layoffs_staging_1
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging_1
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT `date`
FROM layoffs_staging_1;

-- The column Date Data type is currently text, coverting to date format then to date --
SELECT  `date`, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging_1;

UPDATE layoffs_staging_1
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging_1
MODIFY COLUMN `date` DATE;


SELECT DISTINCT country
FROM layoffs_staging_1;

SELECT *
FROM layoffs_staging_1
WHERE country LIKE 'United Sta%';

UPDATE layoffs_staging_1
SET country = 'United States'
WHERE country LIKE 'United Sta%';


-- Handle Nulls and Blanks --
SELECT *
FROM layoffs_staging_1
WHERE company like ''Bally'%';

SELECT *
FROM layoffs_staging_1
WHERE industry  IS NULL
OR industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging_1 t1
JOIN layoffs_staging_1 t2
ON t1.company = t2.company
WHERE t1.industry IS NULL OR t1.industry = ''
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging_1
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging_1 t1
	JOIN layoffs_staging_1 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Company Bally's Interactive is not update because only a row exist --

-- Remove unneeded columns and rows --
-- removing rows having null values in columns total_laid_off and percentage_laid_off --
SELECT *
FROM layoffs_staging_1
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging_1
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Deleting field row_num --
ALTER TABLE layoffs_staging_1
DROP COLUMN row_num;