-- DATA CLEANING
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Check the dataset
SELECT *
FROM layoffs;

-- The first thing I do is create a staging table, which I use for cleaning and processing the data. I keep the original raw data as a backup in case anything goes wrong.
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Steps I usually follow when cleaning data:
-- 1. Remove duplicates
-- 2. Standardize data
-- 3. Handle null or missing values
-- 4. Remove any unnecessary columns or rows

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 1. REMOVE DUPLICATES
-- Identify duplicates using CTE and window function
WITH duplicates AS(
	SELECT *,
    ROW_NUMBER() OVER(
    PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS `ROW_NUMBER`
    FROM layoffs_staging)
SELECT *
FROM duplicates
WHERE `ROW_NUMBER`>1;

-- Create another staging table to temporarily hold the row numbers because MySQL does not allow deleting rows directly from a CTE result.
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
  `row_number` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

-- Insert data into the new table with row numbers
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS `ROW_NUMBER`
FROM layoffs_staging;

-- Verify duplicates were tagged
SELECT *
FROM layoffs_staging2
WHERE `ROW_NUMBER`>1;

-- Remove all duplicate records
DELETE
FROM layoffs_staging2
WHERE `ROW_NUMBER`>1;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 2. STANDARDIZE DATA
-- Check for inconsistencies in company names like extra spaces, misspellings, or variations
SELECT DISTINCT company
FROM layoffs_staging2
ORDER BY 1;

-- Check to see what the TRIM() function will do to the unwanted spaces in company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Clean the company names by removing unwanted spaces
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Check for inconsistencies in industry names like extra spaces, misspellings, or variations
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Identify entries that start with 'Crypto' to catch inconsistent labels
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Unify all related entries into the single term 'Crypto', which improve grouping and analysis accuracy
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Check for inconsistencies in location names like extra spaces, misspellings, or variations
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;
-- Location column is already clean

-- Check for inconsistencies in country names like extra spaces, misspellings, or variations
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Clean up any country values like 'United States.' by removing trailing periods, ensuring consistency in naming
UPDATE layoffs_staging2
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE 'United States%';

-- Converting and Formatting the date column
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 3. HANDLE NULL OR MISSING VALUES
-- Check for industry values that are missing or blank
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Manual checks for specific companies with NULL or blank industry values. Verify whether the same company has entries that do have valid industries.
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- Standardize missing values by converting empty strings into proper NULL values, making them easier to identify and handle uniformly.
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Verify that empty strings were successfully converted to NULL
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Fill missing industry using known values from same company
-- This joins the table to itself to find matching company names
-- One record has NULL in industry
-- Another record for the same company has a valid industry
SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 4. REMOVE ANY UNNECESSARY COLUMNS OR ROWS
-- Review rows with missing values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

-- This narrows it down further to rows where both total_laid_off and percentage_laid_off are missing.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- These records don’t contain any layoff data, so they’re unlikely to add value or contribute meaningfully to the analysis.
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

-- row_number column is no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN `row_number`;

SELECT * 
FROM layoffs_staging2;

-- Some null values in `total_laid_off`, `percentage_laid_off`, and `funds_raised_millions` appear to be normal.
-- I decided not to modify them, as keeping them null makes it easier to handle missing data during the EDA phase.



