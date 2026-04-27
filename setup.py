"""Agent Cluster - Setup Configuration"""

from setuptools import setup, find_packages

setup(
    name='agent-cluster',
    version='0.12.0',
    description='Distributed AI Agent Cluster with capability discovery, auto-learning, peer messaging, OTA updates, and optional OpenClaw integration',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Agent Cluster Team',
    url='https://github.com/fieryeden/agent-cluster',
    license='MIT',
    packages=find_packages(exclude=['tests*', 'build*', 'dist*']),
    python_requires='>=3.8',
    install_requires=[],
    extras_require={
        'dev': [
            'pytest>=7.0',
        ],
        'web': [
            'aiohttp>=3.8',
            'websockets>=10.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'agent-cluster=agent_cluster.__main__:main',
            'agent-dashboard=dashboard.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Distributed Computing',
    ],
    keywords='ai agent distributed automation cluster',
    include_package_data=True,
    zip_safe=False,
)
