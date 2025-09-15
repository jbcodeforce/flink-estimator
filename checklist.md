# Implementation Checklist

This checklist outlines the missing implementation steps to complete the Flink Estimator project based on the PRD.

## Core Features

### Saved Estimations

- [ ] Implement backend logic to save estimations to a persistent storage (e.g., a file or a database).
- [x] Create a POST endpoint to save an estimation.
- [x] Create a GET endpoint to retrieve all saved estimations.
- [x] Create a GET endpoint to retrieve a single estimation by its ID.
- [x] Create a DELETE endpoint to remove a saved estimation.
- [x] Display the list of saved estimations on the "Saved Estimations" page.
- [x] Add functionality to view the details of a saved estimation.

### RESTful API

- [ ] Review and finalize the API endpoints for programmatic access.
- [x] Generate and expose OpenAPI/Swagger documentation for the API.
- [ ] Add comprehensive examples to the API documentation.

## Future Work

### Advanced Estimation Models

- [ ] Research and design a machine learning model for resource estimation.
- [ ] Collect and preprocess data for training the model.
- [ ] Train and evaluate the machine learning model.
- [ ] Integrate the machine learning model into the estimation engine.

### Support for Different Deployment Modes

- [ ] Analyze the resource allocation models for kubernetes deployment.
- [ ] Extend the estimation logic.
- [ ] Add an option to the UI and API to select the deployment mode.

### User Authentication

- [ ] Choose an authentication mechanism (e.g., OAuth2, JWT).
- [ ] Associate saved estimations with user accounts.
- [ ] Protect the API endpoints with authentication.
