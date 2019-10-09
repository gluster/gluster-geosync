from setuptools import setup


setup(
    name="gluster-geosync",
    version="0.1",
    packages=["glustergeosync"],
    include_package_data=True,
    install_requires=['pyxattr'],
    entry_points={
        "console_scripts": [
            "gluster-geosync = glustergeosync.main:main",
        ]
    },
    platforms="linux",
    zip_safe=False,
    author="Gluster Developers",
    author_email="gluster-devel@gluster.org",
    description="GlusterFS Geo synchronization",
    license="Apache-2.0",
    keywords="glusterfs, sync, geosync",
    url="https://github.com/gluster/gluster-geosync",
    long_description="""
    Tool to sync data from one Gluster Volume to another
    remote Gluster Volume asynchronously.
    """,
    classifiers=[
        "Environment :: Console",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
    ],
)
