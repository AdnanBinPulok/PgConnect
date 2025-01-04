from setuptools import setup, find_packages

setup(
    name='pgconnect',
    version='0.0.5',
    author='AdnanBinPulok',
    author_email='adnanbinpulok@gmail.com',
    description='A PostgreSQL connection and ORM library',
    long_description=open('readme.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/AdnanBinPulok/PgConnect',
    packages=find_packages(where='src', include=['pgconnect', 'pgconnect.*']),
    package_dir={'': 'src'},
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=[
        'asyncpg',
    ],
)