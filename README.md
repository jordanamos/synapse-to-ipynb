[![Build Status](https://dev.azure.com/amostj/synapse-to-ipynb/_apis/build/status%2Fjordanamos.synapse-to-ipynb?branchName=main)](https://dev.azure.com/amostj/synapse-to-ipynb/_build/latest?definitionId=12&branchName=main)
# Synapse-to-IPython Notebook (synapse-to-ipynb)

[Azure Synapse Analytics](https://azure.microsoft.com/en-au/products/synapse-analytics/) uses notebooks for various computational tasks however managing these in a local dev environment and with proper version control is difficult. If this is a problem you have run into, this tool might be for you.

This program was designed primarily for use in a CI/CD pipeline.


## Features

- Create new IPython notebooks from Synapse notebooks
- Delete IPython notebooks that don't exist in Synapse notebooks
- Update Synapse notebooks from IPython notebooks

## Installation

```
pip install synapse-to-ipynb
```

## Workflow and Usage

1. Create Synapse notebooks directly in Synapse

2. Create ipynbs from Synapse notebooks. The source Synapse directory is considered the point of truth and so the default behaviour is that if the target already contains .ipynb files, ones that don't have a matching Synapse notebook will be deleted:

    ```
    synapse-to-ipynb --source <synapse_directory> --target <ipython_directory>
    ```

    - `--source`: The source directory containing Synapse notebooks (.json).
    - `--target`: The target directory to store IPython Notebooks (.ipynb).

3. Develop the .ipynb files in your faviourite IDE

4. To migrate the changes back to Synapse use the `--update` flag:
    ```
    synapse-to-ipynb --source <synapse_directory> --target <ipython_directory> --update
    ```
    - `--source`: The source directory containing Synapse notebooks (.json).
    - `--target`: The target directory to store IPython Notebooks (.ipynb).
    - `--update`: Put the tool in update mode

## Contributing

Feedback and contributions are welcome! If you encounter any issues or have suggestions for improvement, please create an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
