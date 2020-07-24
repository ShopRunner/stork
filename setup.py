from setuptools import setup

with open("stork/_version.py") as version_file:
    exec(version_file.read())

with open('README.md') as r:
    readme = r.read()

with open('LICENSE.txt') as l:
    license = l.read()

setup(
    name='stork',
    version=__version__,
    description='Update libraries on Databricks',
    long_description=readme+'\n\n\nLicense\n-------\n'+license,
    long_description_content_type='text/markdown',
    author='Hanna Torrence',
    author_email='data-science@shoprunner.com',
    url='https://github.com/shoprunner/stork',
    license='BSD-3-Clause',
    packages=['stork'],
    data_files=[('', ['LICENSE.txt'])],
    install_requires=[
        'click',
        'click_log',
        'configparser',
        'requests',
        'simplejson'
    ],
    extras_require={
        'dev': [
            'flake8',
            'numpydoc',
            'pytest',
            'pytest-cov',
            'responses',
            'sphinx',
            'sphinxcontrib-programoutput',
        ]
    },
    entry_points={'console_scripts': ['stork = stork.cli:cli']}
)
