import time
import requests
import hashlib
from loguru import logger
from datetime import datetime
from bs4 import BeautifulSoup

class JobsScraper:
    def __init__(self, jobs_to_scrap: list):
        self.jobs_to_scrap = jobs_to_scrap
        
    def __generate_soup(self, url: str) -> BeautifulSoup:
        """
        Generate a BeautifulSoup object from a URL.

        Parameters
        ----------
        url : str
            The URL to scrape.

        Returns
        -------
        BeautifulSoup
            A BeautifulSoup object containing the parsed HTML of the page.
        """
        
        max_retries = 10
        base_delay = 0.5 
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                
                # Handle 429 code (Too Many Requests)
                if response.status_code == 429:
                    wait_time = base_delay * (2 ** attempt)
                    logger.warning(f"Rate limit hit (429). Waiting {wait_time} seconds before retry. Attempt {attempt+1}/{max_retries}. url: {url}")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if the job is not accessible because of applications closed
                if soup.select_one('body > div.base-serp-page'):
                    logger.warning(f"Page not accessible due to closed applications on url: {url}")
                    return None
                
                logger.success(f"Page successfully generated: {url}")
                return soup
                
            except requests.exceptions.RequestException as e:
                # Vérifier si l'exception est due à un code 429
                if hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                    wait_time = base_delay * (2 ** attempt)  # Backoff exponentiel
                    logger.warning(f"Rate limit hit (429). Waiting {wait_time} seconds before retry. Attempt {attempt+1}/{max_retries}")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"Job offer was not found, maybe deleted: {e}")
                    return None
        
        logger.error(f"Failed to retrieve page after {max_retries} attempts: {url}")
        return None
        
    def __get_job_name(self, soup: str) -> str:
        try:
            title_element = soup.select_one('section.top-card-layout h1.top-card-layout__title')
            if title_element:
                return title_element.text.strip().replace("’", "'")
            else:
                logger.error("Title element not found")
                return "ValueNotFound"
        except Exception as e:
            logger.error(f"Error extracting title: {e}")
            return "ValueNotFound       " 
        
    def __get_company_name(self, soup: BeautifulSoup) -> str:
        """
        Extract the company name from the BeautifulSoup object.
    
        Parameters
        ----------
        soup : BeautifulSoup
            A BeautifulSoup object containing the parsed HTML of the job posting.
    
        Returns
        -------
        str
            The company name, or ValueNotFound if not found.
        """
        try:
            company_element = soup.select_one('a.topcard__org-name-link')
            if company_element:
                return company_element.text.strip().replace("’", "'")
            else:
                company_element = soup.select_one('span.topcard__flavor')
                if company_element:
                    return company_element.text.strip().replace("’", "'")
                else:
                    logger.error("Company element not found")
                    return "ValueNotFound"

        except Exception as e:
            logger.error(f"Error extracting company name: {e}")
            return "ValueNotFound"       

    def __get_job_location(self, soup: BeautifulSoup) -> str:
        """
        Extract the job location (city) from the BeautifulSoup object.

        Parameters
        ----------
        soup : BeautifulSoup
            A BeautifulSoup object containing the parsed HTML of the job posting.

        Returns
        -------
        str
            The job location (city), or ValueNotFound if not found.
        """
        try:
            location_element = soup.select_one('span.topcard__flavor.topcard__flavor--bullet')
            if location_element:
                return location_element.text.strip().replace("’", "'")
            else:
                logger.error("Location element not found")
                return "ValueNotFound"
        except Exception as e:
            logger.error(f"Error extracting job location: {e}")
            return "ValueNotFound"

    def __get_information(self, soup: BeautifulSoup, value_to_retreive: str) -> str:
        """
        Extract the job information depending of the value to retreive (e.g., Sector, level, etc.)

        Parameters
        ----------
        soup : BeautifulSoup
            A BeautifulSoup object containing the parsed HTML of the job posting.
        value_to_retreive : str
            The value to retrieve (e.g., 'Niveau hiérarchique', 'Type d’emploi', 'Secteurs').

        Returns
        -------
        str
            The job information, or ValueNotFound if not found.
        """
        try:
            level_header = soup.find('div', class_='decorated-job-posting__details')
            job_criteria_items = level_header.find_all('li', class_='description__job-criteria-item')

            for item in job_criteria_items:
                header_element = item.find('h3', class_='description__job-criteria-subheader')
                if header_element and value_to_retreive in header_element.text.strip():
                    value = item.find('span', class_='description__job-criteria-text description__job-criteria-text--criteria').text.strip().replace("’", "'")
                    return value
        except Exception as e:
            logger.error(f"Error extracting job INFORMATION: {e}")
            return "ValueNotFound"
        
    def __get_job_description(self, soup: BeautifulSoup) -> str:
        """
        Extract the job description from the BeautifulSoup object.

        Parameters
        ----------
        soup : BeautifulSoup
            A BeautifulSoup object containing the parsed HTML of the job posting.

        Returns
        -------
        str
            The job description, or ValueNotFound if not found.
        """
        
        try:
            description_element = soup.select_one('div.show-more-less-html__markup')
            if description_element:
                formatted_text = description_element.get_text(
                    separator='\n', strip=True
                ).replace('\n', ' ').replace("’", "'").replace('"', "'")
                return formatted_text
            else:
                logger.warning("Description element not found")
                return "ValueNotFound"
        except Exception as e:
            logger.error(f"Error extracting job description: {e}")
            return "ValueNotFound"

    def __generate_job_id(self, job_details):
        """
        Generate a unique ID for a job based on month+year, job_name, company_name, and job_location.
        This is used to avoid duplicates in the database.
        Each job is allowed once a month depending on :
        - the job name
        - the company name
        - the job location
        
        Parameters
        ----------
        job_details : dict
            Dictionary containing job details.
        
        Returns
        -------
        str
            A unique hexadecimal ID for the job.
        """
        try:
            # Parse the date to extract month and year
            date_obj = datetime.strptime(job_details['date'], '%Y-%m-%d')
            month_year = f"{date_obj.month:02d}{date_obj.year}"
            job_name = job_details['job_name']
            company_name = job_details['company_name']
            job_location = job_details['job_location']
            
            # Concatenate the fields
            concat_string = f"{month_year}_{job_name}_{company_name}_{job_location}"
            
            # Normalize the string (lowercase, remove extra spaces)
            normalized = ' '.join(concat_string.lower().split())
            
            # Generate a hash
            hash_object = hashlib.md5(normalized.encode())
            return hash_object.hexdigest()
        except Exception as e:
            logger.error(f"Error generating job ID: {e}")
            return None

    def scrape_jobs(self) -> list[dict]:
        """
        Scrape job details from the given jobs list.

        Returns
        -------
        list[dict]
            A list of dictionaries containing job details.
        """

        job_details_list = []

        for job in self.jobs_to_scrap:
            url = job['url'] 
            soup = self.__generate_soup(url)
            
            if soup is None:
                continue
            
            job_details = {
                'date': job['date'],
                'job_scraped': job['job'],
                'job_name': self.__get_job_name(soup=soup),
                'company_name': self.__get_company_name(soup=soup),
                'job_location': self.__get_job_location(soup=soup),
                'job_level': self.__get_information(soup=soup, value_to_retreive='Niveau hiérarchique'),
                'job_type': self.__get_information(soup=soup, value_to_retreive='Type d’emploi'),
                'job_category': self.__get_information(soup=soup, value_to_retreive='Fonction'),
                'job_sector': self.__get_information(soup=soup, value_to_retreive='Secteurs'),
                'job_description' : self.__get_job_description(soup=soup),
                'url': url,
            }
            
            job_details['job_id'] = self.__generate_job_id(job_details)
            
            job_details_list.append(job_details)
            
        return job_details_list
            