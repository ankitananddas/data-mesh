from setuptools import setup

setup(
    name='data_mesh_landing_layer',
    version='0.0.1',
    author="Shubham Upadhyaya",
    author_email="shubham.upadhyaya@coditas.com",
    description="Package to populate the tables in landing and staging layer of basix data-mesh",
    packages=['bsx_data_mesh_etl', 'bsx_data_mesh_etl.constants', 'bsx_data_mesh_etl.utils', 'bsx_data_mesh_etl.tables']
)
