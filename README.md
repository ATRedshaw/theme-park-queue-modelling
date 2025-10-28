# Theme Park Queue Modelling

## Project Overview

This project delves into the fascinating world of theme park queue times, aiming to understand and predict crowd levels and ride wait times. I built this to explore data scraping, time-series analysis, and predictive modelling in a real-world context, focusing on how various factors influence visitor experience. It's a personal endeavour to combine my interests in data science and theme parks, providing insights into efficiency and guest flow.

## Components

### Data Scraping

The `scraping/` module is responsible for gathering raw queue time data from a popular theme park queue tracking website. It uses [Playwright](https://playwright.dev/) to automate browser interactions, mimicking human behaviour to log in and extract data from various park calendars.

Key features:
- **Configurable:** Reads park IDs, date ranges, and excluded months from `config.yml`.
- **Secure Credentials:** Loads login credentials from a `.env` file, ensuring sensitive information isn't hardcoded.
- **Database Integration:** Stores scraped data into a local SQLite database (`data/queue_data.db`), including ride-specific information and queue times at 15-minute intervals.
- **Resilient:** Includes logging and error handling to manage network issues or unexpected page structures.

### Model Development

The `models/` directory houses the predictive models designed to analyse and forecast theme park dynamics. It's split into two main areas:

#### Crowd Level Prediction

The `models/crowd-level/` sub-module focuses on predicting overall crowd levels within the park. This involves:
- **Data Pipelines:** Utilising `utils/pipeline.py` and `utils/preprocess.py` for preparing scraped data, incorporating geographical features (`utils/geo.py`), holiday information (`utils/holidays.py`), and park opening times (`utils/opening.py`).
- **Training:** The `train.py` script is used to develop and train machine learning models.
- **Inference:** `inference.py` handles making predictions based on the trained models, which are exported to `model-exports/`.

#### Queue Time Prediction

The `models/queues/` sub-module is a work-in-progress dedicated to predicting individual ride queue times. It currently includes:
- **Preprocessing:** `preprocessing.py` handles the specific data preparation required for queue time models.
- **Modelling Pipeline:** `pipeline.py` orchestrates the training and prediction processes for these models.


## Setup and Usage

To get this project running locally, follow these steps:

### Prerequisites

You'll need Python 3.8+ and `pip` installed.

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/theme-park-queue-modelling.git
cd theme-park-queue-modelling
```

### 2. Set up Environment Variables

Create a `.env` file in the root directory of the project with your login credentials for the queue-times website:

```
USERNAME=your_email@example.com
PASSWORD=your_password
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
playwright install
```

### 4. Configure the Scraper

Edit the `config.yml` file in the root directory to specify the `start_date`, `end_date`, `exclude_months`, and `park_ids` for the data you wish to scrape.

```yaml
scraper:
  start_date: "YYYY/MM/DD"
  end_date: "YYYY/MM/DD"
  exclude_months: [1, 2, 11, 12] # Example: Exclude Jan, Feb, Nov, Dec
  park_ids: [1, 2] # Example: Park IDs for Alton Towers and Thorpe Park
```

### 5. Run the Scraper

Execute the main scraping script. This will launch a browser, log in, scrape data, and store it in `data/queue_data.db`.

```bash
python scraping/main.py
```

### 6. Run Model Training

After scraping data, you can train the crowd level prediction models. This will process the scraped data, train models for each park, and export them to the `models/crowd-level/model-exports/` directory.

```bash
python models/crowd-level/train.py
```

### 7. Run Model Inference

Once models are trained, you can run inference to get crowd level predictions. This script will load the trained models and generate predictions based on new data.

```bash
python models/crowd-level/inference.py
```

### 8. Run the Dashboard (Optional)

Once you have some data, you can launch the Streamlit dashboard to explore it:

```bash
streamlit run dashboard/app.py
```

## Tech Stack

- **Python:** The primary language for all scripting, scraping, and modelling.
- **Playwright:** Used for robust, headless browser automation in the scraping module.
- **SQLite:** A lightweight, file-based database for storing scraped queue data.
- **Pandas:** Essential for data manipulation and analysis throughout the project.
- **Scikit-learn:** For building and evaluating machine learning models.
- **Streamlit:** Powers the interactive web dashboard for data exploration.
- **`python-dotenv`:** Manages environment variables for secure credential loading.
- **`PyYAML`:** Handles configuration loading from `config.yml`.

## Future Ideas and Notes

- **Expand Park Coverage:** Integrate more theme parks into the scraping and modelling pipeline.
- **Real-time Predictions:** Explore options for near real-time queue time predictions, perhaps using a more dynamic data source or API.
- **Advanced Modelling:** Experiment with more sophisticated time-series models or deep learning approaches for improved accuracy.
- **Interactive Visualisations:** Enhance the Streamlit dashboard with more interactive charts and customisable filters.
- **Deployment:** Consider deploying the scraper and models to a cloud platform for continuous operation.

### Dashboard

The `dashboard/` directory contains a simple web application, `app.py`, built with [Streamlit](https://streamlit.io/). This dashboard serves as a visual interface for exploring the scraped queue data. It's primarily for data exploration and visualisation, allowing for quick insights into historical queue times and park information.
