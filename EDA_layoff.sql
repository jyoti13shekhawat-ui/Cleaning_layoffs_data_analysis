-- EXPLORATORY DATA ANALYSIS

SELECT * 
FROM  layoffs_staging2;


-- TOTAL LAID OFF PEOPLE AND PERCENTAGE 
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- MOST LAID OFF COMPANIES
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT DISTINCT company
FROM layoffs_staging2;

-- MOST LAID OFF INDUSTRIES
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- MOST LAID COUNTRY
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- most laid offs date 
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- MOST LAID OFFS BY STAGE 
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- min/ max date of laid offs
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT substring(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

-- rolling sum of all of this 
WITH rolling_total AS
(
SELECT substring(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_off, 
SUM(total_off) OVER(PARTITION BY `Month`) AS rolling_total
FROM rolling_total; 
/* NOT GOT THE CORRECT ANASWER*/

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM  layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;
 
WITH Company_Year(Company, Years, Total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM  layoffs_staging2
GROUP BY company, YEAR(`date`)
),
Company_year_rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY Total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_year_rank
WHERE Ranking <= 5;





















