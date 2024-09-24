# Job Quality Extractor

This project is about extracting dimensions of job quality from online job adverts. This work was funded by the [Economic Statistics Centre of Excellence](https://www.escoe.ac.uk/).

The term "job quality" refers to aspects of a job that affect worker wellbeing - for example how much the job is paid, and whether the contract is permanent. Most research on job quality rightly focuses on data from the employee's point of view, using surveys or interviews or, recently, [online reviews](https://www.escoe.ac.uk/publications/extracting-dimensions-of-job-quality-from-online-employee-reviews/).

Here, we provide a method for identifying dimensions of job quality in online job adverts.

## What dimensions of job quality do you extract?

We took as our starting point [CIPD's seven dimensions of job quality](https://www.cipd.org/globalassets/media/knowledge/knowledge-hub/reports/2023-pdfs/2023-good-work-index-report-8407.pdf):

1. pay and benefits
2. contract (elsewhere called terms of employment)
3. work-life balance
4. job design and the nature of work
5. relationships at work
6. employee voice
7. health and wellbeing

We also added an additional category, ‘barriers to access’, to our taxonomy, so that dimensions of job quality that directly impact marginalised groups might be gathered together. We made one further addition, “atmosphere, culture and environment”, which fits under “Social support and cohesion” and which we took from Sleeman 2024.

## How does it work?

The pipeline comprises X basic steps:

1. Clean the text minimally, then separate the advert into sentences
2. Classify the sentences as either relating to job quality (eg "We are a friendly supportive team") or not relating to job quality (eg "You must have a friendly supportive demeanour").
3. 

## Usage

To install the package ...

To extract dimensions of job quality from a single job advert or from a list of job adverts ...

## Developer setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter/quickstart), in brief:
  - Install: `direnv` and `conda`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure `pre-commit`
- Download the spacy model: `python -m spacy download en_core_web_sm`

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>This project was made possible via funding from the <a target="_blank" href="https://www.escoe.ac.uk/">Economic Statistics Centre of Excellence</a></p></small>

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>
