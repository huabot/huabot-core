try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

packages = [
    'huabot',
    'huabot.api',
    'huabot.api.route',
    'huabot.robot'
]

requires = ['grapy', 'redis', 'aiobottle', 'beaker', 'aio_periodic']

setup(
    name='huabot',
    version='0.1.2',
    description='a intelligent spider',
    author='Li Meng Jun',
    author_email='lmjubuntu@gmail.com',
    url='',
    packages=packages,
    package_dir={'huabot': 'huabot'},
    include_package_data=True,
    install_requires=requires,
)
