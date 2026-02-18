use capnp::message::Builder;
use capnp::serialize;
use std::net::IpAddr;

use risotto_lib::update::Update;

use crate::update_capnp::update;

pub fn serialize_ip_addr(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(addr) => addr.to_ipv6_mapped().octets(),
        IpAddr::V6(addr) => addr.octets(),
    }
}

pub fn serialize_update_into(update: &Update, output: &mut Vec<u8>) {
    let attrs = update.attrs.as_ref();
    output.clear();

    let router_addr = serialize_ip_addr(update.router_addr);
    let peer_addr = serialize_ip_addr(update.peer_addr);
    let prefix_addr = serialize_ip_addr(update.prefix_addr);
    let next_hop = serialize_ip_addr(
        attrs
            .next_hop
            .unwrap_or(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)),
    );

    let mut message = Builder::new_default();
    {
        let mut u = message.init_root::<update::Builder>();
        u.set_time_received_ns(update.time_received_ns.timestamp_nanos_opt().unwrap() as u64);
        u.set_time_bmp_header_ns(update.time_bmp_header_ns.timestamp_nanos_opt().unwrap() as u64);
        u.set_router_addr(&router_addr);
        u.set_router_port(update.router_port);
        u.set_peer_addr(&peer_addr);
        u.set_peer_bgp_id(update.peer_bgp_id.into());
        u.set_peer_asn(update.peer_asn);
        u.set_prefix_addr(&prefix_addr);
        u.set_prefix_len(update.prefix_len);
        u.set_is_post_policy(update.is_post_policy);
        u.set_is_adj_rib_out(update.is_adj_rib_out);
        u.set_announced(update.announced);
        u.set_synthetic(update.synthetic);

        // BGP Attributes - structured fields
        u.set_origin(&attrs.origin);

        // AS Path
        let mut as_path = u.reborrow().init_as_path(attrs.as_path.len() as u32);
        for (i, &asn) in attrs.as_path.iter().enumerate() {
            as_path.set(i as u32, asn);
        }

        // Next Hop
        u.set_next_hop(&next_hop);

        // Multi Exit Discriminator
        u.set_multi_exit_disc(attrs.multi_exit_discriminator.unwrap_or(0));

        // Local Preference
        u.set_local_preference(attrs.local_preference.unwrap_or(0));

        // Only To Customer
        u.set_only_to_customer(attrs.only_to_customer.unwrap_or(0));

        // Atomic Aggregate
        u.set_atomic_aggregate(attrs.atomic_aggregate);

        // Aggregator
        if let (Some(asn), Some(bgp_id)) = (attrs.aggregator_asn, attrs.aggregator_bgp_id) {
            u.set_aggregator_asn(asn);
            u.set_aggregator_bgp_id(bgp_id);
        }

        // Communities
        let mut communities = u
            .reborrow()
            .init_communities(attrs.communities.len() as u32);
        for (i, &(asn, value)) in attrs.communities.iter().enumerate() {
            let mut community = communities.reborrow().get(i as u32);
            community.set_asn(asn);
            community.set_value(value);
        }

        // Extended Communities
        let mut ext_communities = u
            .reborrow()
            .init_extended_communities(attrs.extended_communities.len() as u32);
        for (i, (type_high, type_low, value)) in attrs.extended_communities.iter().enumerate() {
            let mut ext_community = ext_communities.reborrow().get(i as u32);
            ext_community.set_type_high(*type_high);
            ext_community.set_type_low(*type_low);
            ext_community.set_value(value);
        }

        // Large Communities
        let mut large_communities = u
            .reborrow()
            .init_large_communities(attrs.large_communities.len() as u32);
        for (i, &(global_admin, local_data1, local_data2)) in
            attrs.large_communities.iter().enumerate()
        {
            let mut large_community = large_communities.reborrow().get(i as u32);
            large_community.set_global_admin(global_admin);
            large_community.set_local_data_part1(local_data1);
            large_community.set_local_data_part2(local_data2);
        }

        // Originator ID
        if let Some(originator_id) = attrs.originator_id {
            u.set_originator_id(originator_id);
        }

        // Cluster List
        let mut cluster_list = u
            .reborrow()
            .init_cluster_list(attrs.cluster_list.len() as u32);
        for (i, &cluster_id) in attrs.cluster_list.iter().enumerate() {
            cluster_list.set(i as u32, cluster_id);
        }

        // MP Reach NLRI
        if let Some(afi) = attrs.mp_reach_afi {
            u.set_mp_reach_afi(afi);
        }
        if let Some(safi) = attrs.mp_reach_safi {
            u.set_mp_reach_safi(safi);
        }

        // MP Unreach NLRI
        if let Some(afi) = attrs.mp_unreach_afi {
            u.set_mp_unreach_afi(afi);
        }
        if let Some(safi) = attrs.mp_unreach_safi {
            u.set_mp_unreach_safi(safi);
        }
    }

    serialize::write_message(&mut *output, &message)
        .expect("serializing update into Vec<u8> should not fail");
}

pub fn serialize_update(update: &Update) -> Vec<u8> {
    let mut output = Vec::with_capacity(1024);
    serialize_update_into(update, &mut output);
    output
}
