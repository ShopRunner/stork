from setuptools import setup

with open("apparate/_version.py") as version_file:
    exec(version_file.read())

with open('README.md') as r:
    readme = r.read()

with open('LICENSE.txt') as l:
    license = l.read()

setup(
    name='apparate',
    version=__version__,
    description='Update libraries on Databricks',
    long_description=readme+'\n\n\nLicense\n-------\n'+license,
    long_description_content_type='text/markdown',
    author='Hanna Torrence',
    author_email='htorrence@shoprunner.com',
    url='https://github.com/shoprunner/apparate',
    license='BSD-3-Clause',
    packages=['apparate'],
    data_files=[('', ['LICENSE.txt'])],
    install_requires=[
        'click',
        'click_log',
        'configparser',
        'requests',
        'simplejson'
    ],
    extra_requires={
        'dev': [
            'flake8',
            'm2r',
            'numpydoc',
            'pytest',
            'pytest-cov',
            'responses',
            'sphinx',
            'sphinxcontrib-programoutput',
        ]
    },
    entry_points={'console_scripts': ['apparate = apparate.cli:cli']}
)
