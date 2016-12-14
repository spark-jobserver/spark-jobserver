from setuptools import setup, find_packages
import os

setup(
        name='sjs-python-examples',
        version=os.getenv('SJS_VERSION', 'NO_ENV'),
        description='Examples of jobs for Spark Job Server',
        url='https://github.com/spark-jobserver/spark-jobserver',
        license='Apache License 2.0',
        packages=find_packages(exclude=['test*', 'sparkkjob*']),
        install_requires=['pyhocon', 'py4j']
)
