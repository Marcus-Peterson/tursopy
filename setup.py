from setuptools import setup, find_packages

setup(
    name='tursopy',
    version='0.1.0',
    author='Marcus Peterson',
    author_email='marcus.peterson.tech@gmail.com',
    description='A Python client for Turso API with CRUD and batch operations.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/marcus-peterson/tursopy',
    packages=find_packages(),
    install_requires=[
        'requests',
        'aiohttp',
        'anyio',
'jsonschema',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache 2.0 License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
