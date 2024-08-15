

# PySpark Application

This repository contains a PySpark application that solves the assignment problem described in `de_take_home_test.pdf`. The application processes data to identify users who registered one week before the application was loaded. Please refer to `Edge Cases Explanation.pdf` for more detailed explanations of these edge cases.


## Getting Started

### Setting Up the Environment

1. **Clone the Repository:**

   Clone this repository to your local machine using:

   ```bash
   git clone <repository-url>
   cd <repository-directory>
   python3 -m venv venv
source venv/bin/activate   # On Windows use `venv\Scripts\activate`


2. **Install Dependencies:**
    ```bash
    pip install -r requirements.txt


3. **Running the Application:**
    ```bash
    python3 app.py dataset.json

Replace dataset.json with the path to your dataset file if you want to use a different dataset.

The application consists of two modes:

Parse Mode: Parses and processes the data from the dataset.
Statistics Mode: Computes statistics such as the percentage of users who registered one week after loading the application.

4. **Running Unit Tests:**
    ```bash
    python3 -m unittest tests.py