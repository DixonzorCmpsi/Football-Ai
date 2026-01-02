from applications.api.main import app

if __name__ == "__main__":
    import uvicorn
    # Ensure uvicorn imports the correct module path when running as a script
    uvicorn.run("applications.server:app", host="0.0.0.0", port=8000, reload=True)