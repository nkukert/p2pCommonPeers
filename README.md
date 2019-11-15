# p2pCommonPeers
Hadoop MapReduce program to identify common peers in a gnutella-like p2p system

<h3>Setup Instructions</h3>

Ensure that the hadoop classpath is properly set up. Then compile the program and create the jar as follows:

hadoop com.sun.tools.javac.Main MutualPeerCounter.java

jar cf mpc.jar MutualPeerCounter*.class

Then run the program in your directory, ensuring that the /user/your_name/project_name/input directory contains the three input files, and the /user/your_name/project_name/output directory does not yet exist. Run the program using the following command:

hadoop jar mpc.jar MutualPeerCounter /user/your_name/project_name/input /user/your_name/project_name/output