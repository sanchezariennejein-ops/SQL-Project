-- EXPLORATORY DATA ANALYSIS
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Through this EDA process, we aim to identify significant trends, detect outliers, and recognize meaningful patterns in the dataset.

-- LAYOFFS OVERVIEW
-- Total employees laid off across all companies
SELECT SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2;

-- Average percentage of layoffs per company
SELECT AVG(percentage_laid_off) AS avg_percentage_laid_off 
FROM layoffs;

-- Companies with more than one layoff event
SELECT company, COUNT(*) AS layoffs_count
FROM layoffs_staging2
GROUP BY company
HAVING COUNT(*)>1
ORDER BY 2 DESC;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- INDUSTRY ANALYSIS
-- Industries with the highest total layoffs
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Average layoff percentage by industry
SELECT industry, AVG(percentage_laid_off) AS avg_percentage_laid_off 
FROM layoffs
GROUP BY industry
ORDER BY 2 DESC;

-- Industries with the most frequent layoff events
SELECT industry, COUNT(*) AS layoffs_count
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TIME-BASED TRENDS
-- Months or Years with the most layoffs
SELECT YEAR(date) AS year, MONTH(date) AS month, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY year, month
ORDER BY 3 DESC;

-- Seasonal trend (Quarterly layoffs)
SELECT QUARTER(date) AS quarter, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY quarter
ORDER BY 2 DESC;

-- Layoffs before, during, and after COVID-19
SELECT YEAR(date) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE YEAR(date) BETWEEN 2020 AND 2023
GROUP BY YEAR(date)
ORDER BY 1;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- GEOGRAPHIC INSIGHTS
-- Countries with the most layoffs
SELECT country, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Cities most affected by layoffs
SELECT location, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC;

-- Average percentage laid off by country
SELECT country, AVG(total_laid_off) AS avg_percentage_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY-LEVEL INSIGHTS
-- Companies with the highest number of layoffs
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Companies with highest layoff percentage
SELECT company, MAX(percentage_laid_off) AS max_percent_layoffs
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Companies that laid off 100% of their workforce
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Top 3 companies with the most layoffs for each year
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_layoffs
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_layoffs, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_layoffs DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_layoffs, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY 2 ASC, 3 DESC;

-- Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY dates
ORDER BY 1 ASC;

-- Rolling Total of Layoffs Per Month
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_layoffs) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;


