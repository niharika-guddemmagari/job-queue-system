Distributed Job Queue and Worker Management

About the project

This project is about building a small distributed system that can
receive jobs from different clients and get those jobs completed by a
group of worker computers.

Instead of making one computer do all the work, clients send their jobs
to a central controller. The controller keeps track of the jobs, checks
which workers are available, and assigns the work to them. Once a worker
finishes a job, it sends the result back through the controller to the
client.

We are also exploring how Software-Defined Networking (SDN) can help
monitor the network and find another route when a network path fails.
The Ryu SDN Controller is used for this part of the project.

What we want to achieve

The main aim is to make job processing easier to manage and more
reliable. Through this project, we aim to:

-   Allow multiple clients to connect and submit jobs.
-   Receive jobs and keep track of their progress.
-   Place jobs in a queue until a worker is ready.
-   Assign jobs to workers based on their availability and workload.
-   Let workers execute jobs and return their results.
-   Monitor workers using regular heartbeat messages.
-   Detect when a worker or network connection becomes unavailable.
-   Reassign unfinished jobs to another available worker when possible.
-   Explore SDN-based network monitoring and rerouting using Ryu.

How the system works

Our project has three main parts.

1. Client and job submission — Person 1

The client is the part that the user interacts with. It allows users to
connect to the controller and submit jobs.

This part is responsible for:

-   Connecting to the controller using TCP sockets.
-   Accepting job details from the user.
-   Sending jobs to the controller.
-   Showing confirmation that a job has been received.
-   Displaying job updates and the final result.

2. Central controller, queue and scheduler — Person 2

The controller is the main part of the system. It acts as the link
between clients and workers. It receives jobs, keeps them organised, and
decides which worker should handle each job.

This part is responsible for:

-   Accepting connections from clients and workers.
-   Receiving new jobs and placing them in a queue.
-   Keeping track of registered workers.
-   Checking worker availability and reported workload.
-   Selecting a suitable worker and assigning jobs.
-   Updating job status when work is started or completed.
-   Coordinating job reassignment if a worker fails.
-   Working with the Ryu SDN Controller for network monitoring and
    rerouting.

3. Worker nodes, monitoring and failure handling — Person 3

Workers are the computers or processes that actually perform the
assigned jobs. They receive work from the controller, execute it, and
send the result back.

This part is responsible for:

-   Connecting to the controller and registering as a worker.
-   Receiving and executing assigned jobs.
-   Sending completed results back to the controller.
-   Sending regular heartbeat messages to show that the worker is
    active.
-   Reporting worker status and workload.
-   Helping the controller identify worker failures.

Example: How a job gets completed

Imagine a user submits a job to calculate 20 + 30.

1.  The client sends the job to the controller through a TCP connection.
2.  The controller acknowledges the request and adds the job to the
    queue.
3.  The scheduler checks which workers are available.
4.  The controller assigns the job to one of the available workers.
5.  The worker calculates the result.
6.  The worker sends the result (50) back to the controller.
7.  The controller updates the job status and sends the result to the
    client.

The client can then display that the job has been completed and show the
result.

What happens if a worker fails?

One of the important things we want to demonstrate is how the system
responds when a worker stops working.

For example, suppose a job has been assigned to Worker 2, but Worker 2
suddenly disconnects. The controller can use the worker’s heartbeat
messages and status information to identify that it is no longer
responding.

If the job has not been completed, the controller can put it back into
the queue and assign it to another available worker, such as Worker 3.

The Ryu SDN Controller is used to explore network monitoring and
rerouting when a network path becomes unavailable. This is separate from
worker failure detection, but both features help the system deal with
failures.

FEATURES
-----------
- TCP socket-based communication between application components.
- Client interface for submitting computational jobs.
- Job Manager for coordinating workers and tracking jobs.
- Multiple worker processes.
- Worker registration with the Job Manager.
- Job assignment and result collection.
- Job status/result lookup.
- Worker listing.
- Worker timeout/unavailability handling and retry behaviour.
- Mininet-based network deployment.
- SDN flow-rule installation and verification.
- Performance comparison experiments.

JOB TYPES
------------
1. Submit sum job
2. Submit factorial job
3. Submit maximum job
4. Check job status/result
5. List workers


Technologies

The project may use the following technologies, depending on the final
implementation:

-   Python: For implementing the client, controller and worker
    modules.
-   TCP sockets: For communication between clients, the controller and
    workers.
-   Ryu SDN Controller: For SDN-based network monitoring and control.
-   OpenFlow: For communication between the Ryu controller and
    compatible network switches.
-   Threads and concurrency: To handle multiple connections where
    required.
-   Ubuntu: Development and testing environment.

Team responsibilities

  -----------------------------------------------------------------------
  Team member                         Main area of work
  ----------------------------------- -----------------------------------
  Person 1 (PES1UG25CS232)            Client connection, job submission
                                      and receiving results

  Person 2 (PES1UG25CS193)            Central controller, job queue,
                                      scheduling and SDN integration

  Person 3 (PES1UG25CS235)            Worker implementation, job
                                      execution, monitoring and failure
                                      handling
  -----------------------------------------------------------------------

Each member can develop their part separately at first. We will then
connect the modules through the agreed TCP communication format and test
the complete system together.

project structure

    project/
    |
    |-- client/
    |   |-- client.py
    |   |-- client.h
    |
    |-- controller/
    |   |-- controller.py
    |   |-- queue.py
    |   |-- scheduler.py
    |
    |-- worker/
    |   |-- worker.py
    |   |-- monitor.py
    |
    |-- sdn/
    |   |-- ryu_controller.py
    |
    |-- README.txt

NETWORK CONFIGURATION SHOWN IN THE DEMONSTRATION
---------------------------------------------------

Job Manager : 10.0.0.1:5000
Worker 1    : 10.0.0.2:9001
Worker 2    : 10.0.0.3:9002



Running the project

The exact commands will depend on the programming language, file names
and configuration used in the project. Once the implementation is ready,
follow these general steps:

1.  Clone this repository to your system.
2.  Install the dependencies required by the project.
3.  Configure the IP addresses and port numbers for the controller,
    clients and workers.
4.  Start the Ryu SDN Controller and configure the network, if the SDN
    setup is being used.
5.  Start the central controller.
6.  Start the worker nodes.
7.  Run one or more clients.
8.  Submit a test job and check whether it is assigned, executed and
    returned successfully.

Note: Add the actual installation and run commands here after they have
been tested.

Testing

We will use the following checks to make sure the main parts of the
project work together:

-   ☐ A client can connect to the controller.
-   ☐ Multiple clients can submit jobs.
-   ☐ The controller acknowledges submitted jobs.
-   ☐ Jobs are added to and removed from the queue correctly.
-   ☐ Workers can register with the controller.
-   ☐ Jobs are assigned to available workers.
-   ☐ Workers can execute jobs and return results.
-   ☐ The controller receives worker heartbeat messages.
-   ☐ The system can identify a worker that stops responding.
-   ☐ An unfinished job can be reassigned when another worker is
    available.
-   ☐ SDN monitoring and rerouting can be demonstrated, if configured.
