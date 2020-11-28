/**
  * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
  */
package akka.cluster

import akka.actor.Address

object TestMember {
  def apply(address: Address, status: MemberStatus): Member =
    apply(address, status, Set.empty)

  def apply(address: Address, status: MemberStatus, roles: Set[String]): Member = {
    // According to https://github.com/akka/akka/issues/25496 dc-membership information should be added
    // automatically. For some reason this does not happen and is explicitly added.
    new Member(UniqueAddress(address, 0), Int.MaxValue, status,
      roles ++ Set("dc-default"))
  }
}
