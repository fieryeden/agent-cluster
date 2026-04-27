"""
Agent Cluster - Setup Configuration

For pip install:
    pip install .
    pip install -e .  (development mode)
"""

from setuptools import setup, find_packages

setup(
    name='agent-cluster',
    version='0.9.0',
    description='Distributed AI Agent System - Run AI agents on any Python-capable device',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Agent Cluster Team',
    author_email='team@agentcluster.ai',
    url='https://github.com/agentcluster/agent-cluster',
    license='MIT',
    
    packages=find_packages(exclude=['tests*', 'build*', 'dist*']) + ['dashboard', 'coordinator', 'agents', 'handlers', 'protocol', 'capabilities', 'autolearning', 'reliability', 'security', 'deployment', 'network', 'orchestration'],
    
    python_requires='>=3.8',
    
    # No external dependencies - uses stdlib only
    install_requires=[],
    
    extras_require={
        'dev': [
            'pytest>=7.0',
            'pytest-cov>=4.0',
            'black>=23.0',
            'mypy>=1.0',
        ],
        'web': [
            'aiohttp>=3.8',
            'websockets>=10.0',
        ],
        'ai': [
            'openai>=1.0',
            'anthropic>=0.18',
        ],
    },
    
    entry_points={
        'console_scripts': [
            'agent-cluster=agent_cluster.__main__:main',
            'agent-coordinator=coordinator.server:main',
            'agent-daemon=agents.nanobot:main',
        ],
    },
    
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration',
    ],
    
    keywords='ai agent distributed automation cluster',
    
    include_package_data=True,
    zip_safe=False,
)
