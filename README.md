
# FastAPI Application

This project contains a FastAPI application located within the `src` directory.

## Setup and Installation

### 1. Install Dependencies

To install the required packages for the project, run the following command:

```bash
pip install -r requirements.txt
```

### 2. Environment Variables

Before running the application, you need to set up environment variables. Use the provided `.env.example` as a template:

- Copy the `.env.example` file and rename the copy to `.env`.
  
    ```bash
    cp .env.example .env
    ```

- Modify the `.env` file with your values.

#### VS Code Setup

If you are using VS Code, you can add the `.env` to your run settings:

1. Go to the run configurations (`.vscode/launch.json`).
2. Add the following to your configuration:

    ```json
    "envFile": "${workspaceFolder}/.env"
    ```

This ensures that the environment variables from the `.env` file are loaded when you run or debug your application in VS Code.

## Running the Application

With the environment variables in place, you can run the application as you would any FastAPI application.


## API Docs


Go to http://localhost:8000/docs to test the endpoints
