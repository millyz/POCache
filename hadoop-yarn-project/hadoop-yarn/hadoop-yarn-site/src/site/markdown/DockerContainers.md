<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Launching Applications Using Docker Containers
==============================================

<!-- MACRO{toc|fromDepth=0|toDepth=1} -->

Security Warning
---------------
**IMPORTANT** This feature is experimental and is not complete. **IMPORTANT**
Enabling this feature and running Docker containers in your cluster has security
implications. With this feature enabled, it may be possible to gain root access
to the YARN NodeManager hosts. Given Docker's integration with many powerful
kernel features, it is imperative that administrators understand
[Docker security](https://docs.docker.com/engine/security/security/) before
enabling this feature.

Overview
--------

[Docker](https://www.docker.io/) combines an easy-to-use interface to Linux
containers with easy-to-construct image files for those containers. In short,
Docker enables users to bundle an application together with its preferred
execution environment to be executed on a target machine. For more information
about Docker, see their [documentation](http://docs.docker.com).

The Linux Container Executor (LCE) allows the YARN NodeManager to launch YARN
containers to run either directly on the host machine or inside Docker
containers. The application requesting the resources can specify for each
container how it should be executed. The LCE also provides enhanced security
and is required when deploying a secure cluster. When the LCE launches a YARN
container to execute in a Docker container, the application can specify the
Docker image to be used.

Docker containers provide a custom execution environment in which the
application's code runs, isolated from the execution environment of the
NodeManager and other applications. These containers can include special
libraries needed by the application, and they can have different versions of
native tools and libraries including Perl, Python, and Java. Docker
containers can even run a different flavor of Linux than what is running on the
NodeManager.

Docker for YARN provides both consistency (all YARN containers will have the
same software environment) and isolation (no interference with whatever is
installed on the physical machine).

Docker support in the LCE is still evolving. To track progress, follow
[YARN-3611](https://issues.apache.org/jira/browse/YARN-3611), the umbrella JIRA
for Docker support improvements.

Cluster Configuration
---------------------

The LCE requires that container-executor binary be owned by root:hadoop and have
6050 permissions. In order to launch Docker containers, the Docker daemon must
be running on all NodeManager hosts where Docker containers will be launched.
The Docker client must also be installed on all NodeManager hosts where Docker
containers will be launched and able to start Docker containers.

To prevent timeouts while starting jobs, any large Docker images to be used by
an application should already be loaded in the Docker daemon's cache on the
NodeManager hosts. A simple way to load an image is by issuing a Docker pull
request. For example:

```
    sudo docker pull images/hadoop-docker:latest
```

The following properties should be set in yarn-site.xml:

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.container-executor.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value>
    <description>
      This is the container executor setting that ensures that all applications
      are started with the LinuxContainerExecutor.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.linux-container-executor.group</name>
    <value>hadoop</value>
    <description>
      The POSIX group of the NodeManager. It should match the setting in
      "container-executor.cfg". This configuration is required for validating
      the secure access of the container-executor binary.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users</name>
    <value>false</value>
    <description>
      Whether all applications should be run as the NodeManager process' owner.
      When false, applications are launched instead as the application owner.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.allowed-runtimes</name>
    <value>default,docker</value>
    <description>
      Comma separated list of runtimes that are allowed when using
      LinuxContainerExecutor. The allowed values are default, docker, and
      javasandbox.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.allowed-container-networks</name>
    <value>host,none,bridge</value>
    <description>
      Optional. A comma-separated set of networks allowed when launching
      containers. Valid values are determined by Docker networks available from
      `docker network ls`
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.default-container-network</name>
    <value>host</value>
    <description>
      The network used when launching Docker containers when no
      network is specified in the request. This network must be one of the
      (configurable) set of allowed container networks.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.host-pid-namespace.allowed</name>
    <value>false</value>
    <description>
      Optional. Whether containers are allowed to use the host PID namespace.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed</name>
    <value>false</value>
    <description>
      Optional. Whether applications are allowed to run in privileged
      containers.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.privileged-containers.acl</name>
    <value></value>
    <description>
      Optional. A comma-separated list of users who are allowed to request
      privileged contains if privileged containers are allowed.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.capabilities</name>
    <value>CHOWN,DAC_OVERRIDE,FSETID,FOWNER,MKNOD,NET_RAW,SETGID,SETUID,SETFCAP,SETPCAP,NET_BIND_SERVICE,SYS_CHROOT,KILL,AUDIT_WRITE</value>
    <description>
      Optional. This configuration setting determines the capabilities
      assigned to docker containers when they are launched. While these may not
      be case-sensitive from a docker perspective, it is best to keep these
      uppercase. To run without any capabilites, set this value to
      "none" or "NONE"
    </description>
  </property>
</configuration>
```

In addition, a container-executer.cfg file must exist and contain settings for
the container executor. The file must be owned by root with permissions 0400.
The format of the file is the standard Java properties file format, for example

    `key=value`

The following properties are required to enable Docker support:

|Configuration Name | Description |
|:---- |:---- |
| `yarn.nodemanager.linux-container-executor.group` | The Unix group of the NodeManager. It should match the yarn.nodemanager.linux-container-executor.group in the yarn-site.xml file. |

The container-executor.cfg must contain a section to determine the capabilities that containers
are allowed. It contains the following properties:

|Configuration Name | Description |
|:---- |:---- |
| `module.enabled` | Must be "true" or "false" to enable or disable launching Docker containers respectively. Default value is 0. |
| `docker.binary` | The binary used to launch Docker containers. /usr/bin/docker by default. |
| `docker.allowed.capabilities` | Comma separated capabilities that containers are allowed to add. By default no capabilities are allowed to be added. |
| `docker.allowed.devices` | Comma separated devices that containers are allowed to mount. By default no devices are allowed to be added. |
| `docker.allowed.networks` | Comma separated networks that containers are allowed to use. If no network is specified when launching the container, the default Docker network will be used. |
| `docker.allowed.ro-mounts` | Comma separated directories that containers are allowed to mount in read-only mode. By default, no directories are allowed to mounted. |
| `docker.allowed.rw-mounts` | Comma separated directories that containers are allowed to mount in read-write mode. By default, no directories are allowed to mounted. |
| `docker.host-pid-namespace.enabled` | Set to "true" or "false" to enable or disable using the host's PID namespace. Default value is "false". |
| `docker.privileged-containers.enabled` | Set to "true" or "false" to enable or disable launching privileged containers. Default value is "false". |
| `docker.trusted.registries` | Comma separated list of trusted docker registries for running trusted privileged docker containers.  By default, no registries are defined. |
| `docker.inspect.max.retries` | Integer value to check docker container readiness.  Each inspection is set with 3 seconds delay.  Default value of 10 will wait 30 seconds for docker container to become ready before marked as container failed. |
| `docker.no-new-privileges.enabled` | Enable/disable the no-new-privileges flag for docker run. Set to "true" to enable, disabled by default. |

Please note that if you wish to run Docker containers that require access to the YARN local directories, you must add them to the docker.allowed.rw-mounts list.

In addition, containers are not permitted to mount any parent of the container-executor.cfg directory in read-write mode.

The following properties are optional:

|Configuration Name | Description |
|:---- |:---- |
| `min.user.id` | The minimum UID that is allowed to launch applications. The default is no minimum |
| `banned.users` | A comma-separated list of usernames who should not be allowed to launch applications. The default setting is: yarn, mapred, hdfs, and bin. |
| `allowed.system.users` | A comma-separated list of usernames who should be allowed to launch applications even if their UIDs are below the configured minimum. If a user appears in allowed.system.users and banned.users, the user will be considered banned. |
| `feature.tc.enabled` | Must be "true" or "false". "false" means traffic control commands are disabled. "true" means traffic control commands are allowed. |

Part of a container-executor.cfg which allows Docker containers to be launched is below:

```
yarn.nodemanager.linux-container-executor.group=yarn
[docker]
  module.enabled=true
  docker.privileged-containers.enabled=true
  docker.trusted.registries=centos
  docker.allowed.capabilities=SYS_CHROOT,MKNOD,SETFCAP,SETPCAP,FSETID,CHOWN,AUDIT_WRITE,SETGID,NET_RAW,FOWNER,SETUID,DAC_OVERRIDE,KILL,NET_BIND_SERVICE
  docker.allowed.networks=bridge,host,none
  docker.allowed.ro-mounts=/sys/fs/cgroup
  docker.allowed.rw-mounts=/var/hadoop/yarn/local-dir,/var/hadoop/yarn/log-dir

```

Docker Image Requirements
-------------------------

In order to work with YARN, there are two requirements for Docker images.

First, the Docker container will be explicitly launched with the application
owner as the container user.  If the application owner is not a valid user
in the Docker image, the application will fail. The container user is specified
by the user's UID. If the user's UID is different between the NodeManager host
and the Docker image, the container may be launched as the wrong user or may
fail to launch because the UID does not exist.

Second, the Docker image must have whatever is expected by the application
in order to execute.  In the case of Hadoop (MapReduce or Spark), the Docker
image must contain the JRE and Hadoop libraries and have the necessary
environment variables set: JAVA_HOME, HADOOP_COMMON_PATH, HADOOP_HDFS_HOME,
HADOOP_MAPRED_HOME, HADOOP_YARN_HOME, and HADOOP_CONF_DIR. Note that the
Java and Hadoop component versions available in the Docker image must be
compatible with what's installed on the cluster and in any other Docker images
being used for other tasks of the same job. Otherwise the Hadoop components
started in the Docker container may be unable to communicate with external
Hadoop components.

If a Docker image has a
[command](https://docs.docker.com/engine/reference/builder/#cmd)
set, the behavior will depend on whether the
`YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE` is set to true. If so,
the command will be overridden when LCE launches the image with YARN's
container launch script.

If a Docker image has an
[entry point](https://docs.docker.com/engine/reference/builder/#entrypoint)
set, the entry point will be honored, but the default command may be
overridden, as just mentioned above. Unless the entry point is
something similar to `sh -c` or
`YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE` is set to true, the net
result will likely be undesirable. Because the YARN container launch script
is required to correctly launch the YARN task, use of entry points is
discouraged.

If an application requests a Docker image that has not already been loaded by
the Docker daemon on the host where it is to execute, the Docker daemon will
implicitly perform a Docker pull command. Both MapReduce and Spark assume that
tasks which take more that 10 minutes to report progress have stalled, so
specifying a large Docker image may cause the application to fail.

Application Submission
----------------------

Before attempting to launch a Docker container, make sure that the LCE
configuration is working for applications requesting regular YARN containers.
If after enabling the LCE one or more NodeManagers fail to start, the cause is
most likely that the ownership and/or permissions on the container-executer
binary are incorrect. Check the logs to confirm.

In order to run an application in a Docker container, set the following
environment variables in the application's environment:

| Environment Variable Name | Description |
| :------------------------ | :---------- |
| `YARN_CONTAINER_RUNTIME_TYPE` | Determines whether an application will be launched in a Docker container. If the value is "docker", the application will be launched in a Docker container. Otherwise a regular process tree container will be used. |
| `YARN_CONTAINER_RUNTIME_DOCKER_IMAGE` | Names which image will be used to launch the Docker container. Any image name that could be passed to the Docker client's run command may be used. The image name may include a repo prefix. |
| `YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE` | Controls whether the Docker container's default command is overridden.  When set to true, the Docker container's command will be "bash _path\_to\_launch\_script_". When unset or set to false, the Docker container's default command is used. |
| `YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK` | Sets the network type to be used by the Docker container. It must be a valid value as determined by the yarn.nodemanager.runtime.linux.docker.allowed-container-networks property. |
| `YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_PID_NAMESPACE` | Controls which PID namespace will be used by the Docker container. By default, each Docker container has its own PID namespace. To share the namespace of the host, the yarn.nodemanager.runtime.linux.docker.host-pid-namespace.allowed property must be set to true. If the host PID namespace is allowed and this environment variable is set to host, the Docker container will share the host's PID namespace. No other value is allowed. |
| `YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER` | Controls whether the Docker container is a privileged container. In order to use privileged containers, the yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed property must be set to true, and the application owner must appear in the value of the yarn.nodemanager.runtime.linux.docker.privileged-containers.acl property. If this environment variable is set to true, a privileged Docker container will be used if allowed. No other value is allowed, so the environment variable should be left unset rather than setting it to false. |
| `YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS` | Adds additional volume mounts to the Docker container. The value of the environment variable should be a comma-separated list of mounts. All such mounts must be given as "source:dest:mode" and the mode must be "ro" (read-only) or "rw" (read-write) to specify the type of access being requested. The requested mounts will be validated by container-executor based on the values set in container-executor.cfg for docker.allowed.ro-mounts and docker.allowed.rw-mounts. |
| `YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL` | Allows a user to request delayed deletion of the Docker container on a per container basis. If true, Docker containers will not be removed until the duration defined by yarn.nodemanager.delete.debug-delay-sec has elapsed. Administrators can disable this feature through the yarn-site property yarn.nodemanager.runtime.linux.docker.delayed-removal.allowed. This feature is disabled by default. When this feature is disabled or set to false, the container will be removed as soon as it exits. |

The first two are required. The remainder can be set as needed. While
controlling the container type through environment variables is somewhat less
than ideal, it allows applications with no awareness of YARN's Docker support
(such as MapReduce and Spark) to nonetheless take advantage of it through their
support for configuring the application environment.

Once an application has been submitted to be launched in a Docker container,
the application will behave exactly as any other YARN application. Logs will be
aggregated and stored in the relevant history server. The application life cycle
will be the same as for a non-Docker application.

Using Docker Bind Mounted Volumes
---------------------------------

**WARNING** Care should be taken when enabling this feature. Enabling access to
directories such as, but not limited to, /, /etc, /run, or /home is not
advisable and can result in containers negatively impacting the host or leaking
sensitive information. **WARNING**

Files and directories from the host are commonly needed within the Docker
containers, which Docker provides through
[volumes](https://docs.docker.com/engine/tutorials/dockervolumes/).
Examples include localized resources, Apache Hadoop binaries, and sockets. To
facilitate this need, YARN-6623 added the ability for administrators to set a
whitelist of host directories that are allowed to be bind mounted as volumes
into containers. YARN-5534 added the ability for users to supply a list of
mounts that will be mounted into the containers, if allowed by the
administrative whitelist.

In order to make use of this feature, the following must be configured.

* The administrator must define the volume whitelist in container-executor.cfg by setting `docker.allowed.ro-mounts` and `docker.allowed.rw-mounts` to the list of parent directories that are allowed to be mounted.
* The application submitter requests the required volumes at application submission time using the `YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS` environment variable.

The administrator supplied whitelist is defined as a comma separated list of
directories that are allowed to be mounted into containers. The source directory
supplied by the user must either match or be a child of the specified
directory.

The user supplied mount list is defined as a comma separated list in the form
*source*:*destination*:*mode*. The source is the file or directory on the host.
The destination is the path within the contatiner where the source will be bind
mounted. The mode defines the mode the user expects for the mount, which can be
ro (read-only) or rw (read-write).

The following example outlines how to use this feature to mount the commonly
needed /sys/fs/cgroup directory into the container running on YARN.

The administrator sets docker.allowed.ro-mounts in container-executor.cfg to
"/sys/fs/cgroup". Applications can now request that "/sys/fs/cgroup" be mounted
from the host into the container in read-only mode.

At application submission time, the YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS
environment variable can then be set to request this mount. In this example,
the environment variable would be set to "/sys/fs/cgroup:/sys/fs/cgroup:ro".
The destination path is not restricted, "/sys/fs/cgroup:/cgroup:ro" would also
be valid given the example admin whitelist.

Privileged Container Security Consideration
-------------------------------------------

Privileged docker container can interact with host system devices.  This can cause harm to host operating system without proper care.  In order to mitigate risk of allowing privileged container to run on Hadoop cluster, we implemented a controlled process to sandbox unauthorized privileged docker images.

The default behavior is disallow any privileged docker containers.  When `docker.privileged-containers.enabled` is set to enabled, docker image can run with root privileges in the docker container, but access to host level devices are disabled.  This allows developer and tester to run docker images from internet without causing harm to host operating system.

When docker images have been certified by developers and testers to be trustworthy.  The trusted image can be promoted to trusted docker registry.  System administrator can define `docker.trusted.registries`, and setup private docker registry server to promote trusted images.

Trusted images are allowed to mount external devices such as HDFS via NFS gateway, or host level Hadoop configuration.  If system administrators allow writing to external volumes using `docker.allow.rw-mounts directive`, privileged docker container can have full control of host level files in the predefined volumes.

For [YARN Service HTTPD example](./yarn-service/Examples.html), container-executor.cfg must define centos docker registry to be trusted for the example to run.

Container Reacquisition Requirements
------------------------------------
On restart, the NodeManager, as part of the NodeManager's recovery process, will
validate that a container is still running by checking for the existence of the
container's PID directory in the /proc filesystem. For security purposes,
operating system administrator may enable the _hidepid_ mount option for the
/proc filesystem. If the _hidepid_ option is enabled, the _yarn_ user's primary
group must be whitelisted by setting the gid mount flag similar to below.
Without the _yarn_ user's primary group whitelisted, container reacquisition
will fail and the container will be killed on NodeManager restart.

```
proc     /proc     proc     nosuid,nodev,noexec,hidepid=2,gid=yarn     0 0
```

Connecting to a Secure Docker Repository
----------------------------------------

The Docker client command will draw its configuration from the default location,
which is $HOME/.docker/config.json on the NodeManager host. The Docker
configuration is where secure repository credentials are stored, so use of the
LCE with secure Docker repos is discouraged using this method.

YARN-5428 added support to Distributed Shell for securely supplying the Docker
client configuration. See the Distributed Shell help for usage. Support for
additional frameworks is planned.

As a work-around, you may manually log the Docker daemon on every NodeManager
host into the secure repo using the Docker login command:

```
  docker login [OPTIONS] [SERVER]

  Register or log in to a Docker registry server, if no server is specified
  "https://index.docker.io/v1/" is the default.

  -e, --email=""       Email
  -p, --password=""    Password
  -u, --username=""    Username
```

Note that this approach means that all users will have access to the secure
repo.

Example: MapReduce
------------------

To submit the pi job to run in Docker containers, run the following commands:

```
    vars="YARN_CONTAINER_RUNTIME_TYPE=docker,YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=hadoop-docker"
    hadoop jar hadoop-examples.jar pi -Dyarn.app.mapreduce.am.env=$vars \
        -Dmapreduce.map.env=$vars -Dmapreduce.reduce.env=$vars 10 100
```

Note that the application master, map tasks, and reduce tasks are configured
independently. In this example, we are using the hadoop-docker image for all
three.

Example: Spark
--------------

To run a Spark shell in Docker containers, run the following command:

```
    spark-shell --master yarn --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
        --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=hadoop-docker \
        --conf spark.yarn.AppMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=hadoop-docker \
        --conf spark.yarn.AppMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker
```

Note that the application master and executors are configured
independently. In this example, we are using the hadoop-docker image for both.

Docker Container ENTRYPOINT Support
------------------------------------

When Docker support was introduced to Hadoop 2.x, the platform was designed to
run existing Hadoop programs inside Docker container.  Log redirection and
environment setup are integrated with Node Manager.  In Hadoop 3.x, Hadoop
Docker support extends beyond running Hadoop workload, and support Docker container
in Docker native form using ENTRYPOINT from dockerfile.  Application can decide to
support YARN mode as default or Docker mode as default by defining
YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE environment variable.
System administrator can also set as default setting for the cluster to make
ENTRY_POINT as default mode of operation.

In yarn-site.xml, add YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE to
node manager environment white list:
```
<property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME,YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE</value>
</property>
```

In yarn-env.sh, define:
```
export YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE=true
```
