from setuptools import setup


setup(
    name="skewer",
    version="0.1.0",
    packages=["skewer"],
    license="MIT",
    long_description="skewer provides optimizations for joins on skewed keys.",
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)
