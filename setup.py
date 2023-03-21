from setuptools import setup, find_packages

setup(
    name='BackItUp',
    version='0.1',
    description='Python Distribution Utilities',
    packages=find_packages(),
    author='Dominic Rausch',
    # author_email='',
    license='GNU General Public License Version 3.0',
    long_description=open('README.md').read(),
    scripts=['backitup/biu.py'],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: System :: Archiving :: Backup",
        "Operating System :: POSIX :: Linux",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)"
    ],
)
