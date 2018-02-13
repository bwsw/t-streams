CheckpointGroup API
====================

A CheckpointGroup is a feature why we started T-streams. It is a very simple but powerful concept. It provides atomic checkpoint amongst several producers and consumers which share the same metadata store. Please, take a look in a picture below to get an idea of the checkpoint group concept.

.. figure:: _static/CheckpointGroup1.png

Just imagine that a developer has a processing block (module, package, class, etc.) which gets several data streams from subscribers, consumers, then does some useful work inside and outputs results in several output data streams to producers.

A CheckpointGroup allows adding those subscribers, consumers, and producers to the checkpoint group and just call the checkpoint method for the group rather than for each of them separately and this method provides an atomic checkpoint. So, this gives a developer an ability to handle data inside the processing box exactly once and if there are no side effects in the processing unit then the processing unit is fully idempotent. If it crashes somewhere it will start from the last checkpoint.

.. Contents::

add method
---------------

*(updated)*

Adds a producer, a consumer or a subscriber to the group.

::

 def add(agent: GroupParticipant): CheckpointGroup



def cancel(): Unit
--------------------------
*(to add)*

def checkpoint(): Unit
-----------------------------
*(to add)*

clear method
---------------
*(updated)*

Clears the group.

::

 def clear(): CheckpointGroup


val executors: Int
---------------------

exists method
-----------------------

Checks if a producer, a consumer or a subscriber exists inside the group.

::
 
 def exists(name: String): Boolean


remove method
------------------
*(updated)*

Removes a producer, a consumer or a subscriber from the group.

::

 def remove(name: String): CheckpointGroup


stop method
-------------------
*(is added)*

::

 def stop(): Unit


