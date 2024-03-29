# @Author: Kehao Wu <wukehao>
# @Date:   2017-04-06T09:53:40+08:00



from setuptools import setup

setup(name='pgdb',
      version='0.0.11',
      description="PostgreSQL wrapper",
      long_description="",
      classifiers=["Development Status :: 5 - Production/Stable",
                   "License :: OSI Approved :: Apache Software License",
                   "Programming Language :: Python :: 3.5",
                   "Programming Language :: SQL",
                   "Topic :: Database"],
      keywords='psql postgres postgresql sql wrapper',
      author='@wukehao',
      author_email='kehao.wu@gmail.com',
      url='https://github.com/KehaoWu/pgdb',
      license='Apache v2.0',
      packages=['pgdb'],
      include_package_data=True,
      zip_safe=True,
      install_requires=["psycopg2-binary>=2.8.2"],
      entry_points="")
