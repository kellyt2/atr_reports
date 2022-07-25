from setuptools import find_packages, setup

setup(
    name='atr_report_cc',
    packages=find_packages(),
    version='0.1.0',
    description='Finding Sportsbook volume and exchange commission taken fromUKI customers on AUS/NZ/SA Racing. Contains standard RT_Analytics module and project specific modules for atr_report_cc.',
    author='Tadgh Kelly',
    license='',
    install_requires=[
        "python-dotenv >= 0.10.3",
        "anyconfig >= 0.9.10",
        "pyyaml >= 5.1.2",
        "sphinx >= 2.2.1",
        "sphinx-rtd-theme >= 0.4.3",
        "pandas >= 0.25.3",
        "numpy >= 1.17.3",
        "matplotlib >= 3.1.1",
        "seaborn >= 0.9.0",
        "psycopg2 >= 2.8.4",
        "cx-oracle >= 7.2.3",
        "jinja2 >= 2.10.3",
    ],
)
