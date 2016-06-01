package com.bwsw.tstreams.agents.group

import com.bwsw.tstreams.metadata.MetadataStorage

/**
 * Trait which can be implemented by any producer/consumer to apply group checkpoint 
 */
trait Agent {
  /**
   * Info to commit
   */
  def getCommitInfo() : List[CommitInfo]

  /**
   * Metadata storage link for concrete agent
   */
  def getMetadataRef() : MetadataStorage

}
