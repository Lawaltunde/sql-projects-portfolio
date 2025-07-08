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