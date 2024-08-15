

# PySpark Application

Pyspark Application Solution to Assignment description provided in the file. Please refer to the file Edge Cases Explanations for more explanation on identified edge cases. 

## Getting Started

### Prerequisites

Before you can run the application, ensure you have the following installed on your system:

- **Python 3.7 or later:** You can download it from the [official Python website](https://www.python.org/downloads/).
- **Java:** Spark requires Java 8 or later. You can download it from the [official Oracle website](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html) or use a package manager.
- **Apache Spark:** Spark is included with PySpark, but make sure Java is properly set up for it.
- **pip:** The Python package installer. It usually comes with Python, but you can install it separately if needed.

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




