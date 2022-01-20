use base16;
use ergotree_ir::chain::address::Address;
use ergotree_ir::chain::address::AddressEncoder;
use ergotree_ir::chain::address::NetworkPrefix;
use ergotree_ir::chain::base16_bytes::Base16DecodedBytes;
use ergotree_ir::chain::base16_bytes::Base16EncodedBytes;
use ergotree_ir::ergo_tree::ErgoTree;
use ergotree_ir::mir::constant::Constant;
use ergotree_ir::mir::constant::Literal;
use ergotree_ir::mir::value::CollKind;
use ergotree_ir::mir::value::Value;
use ergotree_ir::serialization::SigmaSerializable;
use ergotree_ir::sigma_protocol::sigma_boolean::SigmaBoolean;
use ergotree_ir::sigma_protocol::sigma_boolean::SigmaProofOfKnowledgeTree;
use ergotree_ir::types::stype::SType;

pub(super) fn base16_to_address(base16_str: &str) -> String {
    // let base16_str = "0008cd027304abbaebe8bb3a9e963dfa9fa4964d7d001e6a1bd225eadc84048ae49b627c";
    let tree_bytes = base16::decode(base16_str.as_bytes()).unwrap();
    let tree = ErgoTree::sigma_parse_bytes(&tree_bytes).unwrap();
    let recreated = Address::recreate_from_ergo_tree(&tree).unwrap();
    let encoder = AddressEncoder::new(NetworkPrefix::Mainnet);
    encoder.address_to_str(&recreated)
}

pub(super) fn render_register_value(base16_str: &str) -> String {
    let bytes = base16::decode(base16_str.as_bytes()).unwrap();
    let cst = Constant::sigma_parse_bytes(&bytes).unwrap();
    let val = Value::from(cst.v);
    render_register_val(&val)
}
pub(super) fn render_register_val(val: &Value) -> String {
    // values.rs line 215
    match val {
        Value::Boolean(b) => b.to_string().to_uppercase(),
        Value::Byte(i8) => format!("{:02x}", *i8 as u8),
        Value::Short(i16) => i16.to_string(),
        Value::Int(i32) => i32.to_string(),
        Value::Long(i64) => i64.to_string(),
        Value::BigInt(bi256) => format!("CBigInt({})", bi256),
        Value::GroupElement(e) => base16::encode_lower(&e.sigma_serialize_bytes().unwrap()),
        Value::SigmaProp(sp) => {
            let sb = sp.value();
            match sb {
                SigmaBoolean::TrivialProp(bool) => bool.to_string(),
                SigmaBoolean::ProofOfKnowledge(tree) => match tree {
                    SigmaProofOfKnowledgeTree::ProveDhTuple(dh) => {
                        unimplemented!("ProveDhTuple: {:?}", dh)
                    }
                    SigmaProofOfKnowledgeTree::ProveDlog(dlog) => {
                        base16::encode_lower(&dlog.h.sigma_serialize_bytes().unwrap())
                    }
                },
                SigmaBoolean::SigmaConjecture(conj) => {
                    unimplemented!("SigmaConjecture: {:?}", conj)
                }
            }
        }
        // TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        Value::AvlTree(tree) => unimplemented!("Unhandled AvlTree variant: {:?}", tree),
        Value::CBox(ergo_box) => unimplemented!("Unhandled CBox variant: {:?}", ergo_box),
        Value::Opt(opt) => unimplemented!("Unhandled Option variant: {:?}", opt),
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        Value::Coll(coll) => {
            let raw_values = coll.as_vec();
            let rendered_values: Vec<String> =
                raw_values.iter().map(|v| render_register_val(v)).collect();
            let contains_bytes = match coll {
                CollKind::NativeColl(_) => true,
                CollKind::WrappedColl { elem_tpe, .. } => match elem_tpe {
                    SType::SByte => true,
                    _ => false,
                },
            };
            let sep = match contains_bytes {
                true => "",
                false => ",",
            };
            match contains_bytes {
                true => format!("{}", rendered_values.join(sep)),
                false => format!("[{}]", rendered_values.join(sep)),
            }
        }
        Value::Tup(items) => {
            let rendered_values: Vec<String> =
                items.iter().map(|v| render_register_val(v)).collect();
            format!("[{}]", rendered_values.join(","))
        }
        // Value comes from a Constant, so remaining Value variants (Context,
        // Header, PreHeader, Global and Lambda) should not occur.
        _ => unimplemented!("Unhandled Value variant: {:?}", val),
    }
}

#[cfg(test)]
mod tests {
    use super::base16_to_address;
    use super::render_register_value;
    use pretty_assertions::assert_eq;

    #[test]
    fn address_from_ergo_tree() {
        let ergo_tree = "0008cd03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8";
        assert_eq!(
            base16_to_address(ergo_tree),
            "9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b"
        );
    }

    #[test]
    fn render_register_value_long() {
        let base16_str = "05a4c3edd9998877";
        assert_eq!(render_register_value(base16_str), "261824656027858");
    }

    #[test]
    fn render_register_value_group_element() {
        let base16_str = "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8";
        assert_eq!(
            render_register_value(base16_str),
            "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"
        );
    }

    #[test]
    fn render_register_value_coll_of_byte() {
        let base16_str = "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8";
        assert_eq!(
            render_register_value(base16_str),
            "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"
        );
    }

    // Following tests are based on serialized and rendered values obtained from
    // the explorer backend db with this query:
    //
    // with collected as (
    //     select value_type
    //         , count(*) as cnt
    //         , array_agg(serialized_value) as serialized_value
    //         , array_agg(rendered_value) as rendered_value
    //     from box_registers
    //     group by 1
    // )
    // select value_type
    //     , cnt
    //     , serialized_value[1] as serialized
    //     , rendered_value[1] as rendered
    // from collected
    // order by 1;

    #[test]
    // Coll[Coll[SByte]]
    fn render_register_coll_of_coll_byte() {
        let base16_str = "1a0332537570706f72742074686520726166666c6527732055492f555820646576656c6f706d656e742c206279206d68735f73616da205417320796f752063616e20736565207468652055492f5558206f662074686973207365727669636520697320766572792062617369632e20497420697320626f74686572696e6720626f746820796f7520616e642075732e204c6574277320737461727420746869732073657276696365207769746820796f757220646f6e6174696f6e7320666f722055492f555820646576656c6f706d656e742e0a0a5765206e65656420746f207261697365203135302045524720666f72207468697320707572706f736520616e642074686520726169736564206d6f6e65792077696c6c20626520646f6e6174656420746f20636f6d6d756e697479206465767320746f206372656174652061207375697461626c6520616e64206f70656e20736f757263652055492f555820666f72207468697320736572766963652e200a0a4966207261697365642066756e64732077657265206d6f7265207468616e20313530204552472c207468652065786365737320616d6f756e742077696c6c206265207573656420666f722055492f555820616e642f6f72206f7468657220646576656c6f706d656e74732072656c6174656420746f204572676f20526166666c652e0a0a49276d206d68735f73616d2c204572676f20466f756e646174696f6e20426f617264204d656d62657220616e642074686520666f756e646572206f66204572676f20526166666c652e200a0a546869732066756e6472616973696e6720697320706572736f6e616c20616e6420666f7220746865206d656e74696f6e656420676f616c20616e6420686173206e6f7468696e6720746f20646f2077697468204572676f20466f756e646174696f6e2e0a0a596f752063616e2066696e64206d652061743a2068747470733a2f2f747769747465722e636f6d2f6d68735f73616d201eca1d77eebdb0e9096fdecb6e047ee2169e7c9aef97b0721ad96662f9504bce";
        assert_eq!(
            render_register_value(base16_str),
            "[537570706f72742074686520726166666c6527732055492f555820646576656c6f706d656e742c206279206d68735f73616d,417320796f752063616e20736565207468652055492f5558206f662074686973207365727669636520697320766572792062617369632e20497420697320626f74686572696e6720626f746820796f7520616e642075732e204c6574277320737461727420746869732073657276696365207769746820796f757220646f6e6174696f6e7320666f722055492f555820646576656c6f706d656e742e0a0a5765206e65656420746f207261697365203135302045524720666f72207468697320707572706f736520616e642074686520726169736564206d6f6e65792077696c6c20626520646f6e6174656420746f20636f6d6d756e697479206465767320746f206372656174652061207375697461626c6520616e64206f70656e20736f757263652055492f555820666f72207468697320736572766963652e200a0a4966207261697365642066756e64732077657265206d6f7265207468616e20313530204552472c207468652065786365737320616d6f756e742077696c6c206265207573656420666f722055492f555820616e642f6f72206f7468657220646576656c6f706d656e74732072656c6174656420746f204572676f20526166666c652e0a0a49276d206d68735f73616d2c204572676f20466f756e646174696f6e20426f617264204d656d62657220616e642074686520666f756e646572206f66204572676f20526166666c652e200a0a546869732066756e6472616973696e6720697320706572736f6e616c20616e6420666f7220746865206d656e74696f6e656420676f616c20616e6420686173206e6f7468696e6720746f20646f2077697468204572676f20466f756e646174696f6e2e0a0a596f752063616e2066696e64206d652061743a2068747470733a2f2f747769747465722e636f6d2f6d68735f73616d,1eca1d77eebdb0e9096fdecb6e047ee2169e7c9aef97b0721ad96662f9504bce]"
        );
    }

    #[test]
    // Coll[(Coll[SByte], Coll[SByte])]
    fn render_register_coll_of_tuple_of_two_coll_byte() {
        let base16_str = "0c3c0e0e01240008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb7333967556962486f616569774b5a53707967685a4536594d455a564a753977734b7a465332335778525671366e7a547663476f55";
        assert_eq!(
            render_register_value(base16_str),
            "[[0008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb7,3967556962486f616569774b5a53707967685a4536594d455a564a753977734b7a465332335778525671366e7a547663476f55]]"
        );
    }

    #[test]
    // Coll[(Coll[SByte], Coll[SLong])]
    fn render_register_coll_of_tuple_of_coll_byte_and_coll_long() {
        let base16_str = "0c3c0e1101240008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb703028084af5f00";
        assert_eq!(
            render_register_value(base16_str),
            "[[0008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb7,[1,100000000,0]]]"
        );
    }

    #[test]
    // Coll[(Coll[SByte], SInt)]
    fn render_register_coll_of_tuple_of_coll_byte_and_coll_int() {
        let base16_str =
            "0c4c0e01240008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb714";
        assert_eq!(
            render_register_value(base16_str),
            "[[0008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb7,10]]"
        );
    }

    #[test]
    // (Coll[SByte], Coll[SByte])
    fn render_register_tuple_of_coll_byte_and_coll_byte() {
        let base16_str = "3c0e0e42697066733a2f2f62616679626569666d717579686e6f73786e6b7a62666c6e74687775796675767137756e6b3566727374633633327836767662647962676333697542697066733a2f2f6261666b726569657032616a63756165716c767a6776696468743436367a64776a686f7570707773643779707a33737a6778637433367a3661656d";
        assert_eq!(render_register_value(base16_str), "[697066733a2f2f62616679626569666d717579686e6f73786e6b7a62666c6e74687775796675767137756e6b35667273746336333278367676626479626763336975,697066733a2f2f6261666b726569657032616a63756165716c767a6776696468743436367a64776a686f7570707773643779707a33737a6778637433367a3661656d]");
    }

    #[test]
    // Coll[SInt]
    fn render_register_coll_int() {
        let base16_str = "100204a00b";
        assert_eq!(render_register_value(base16_str), "[2,720]");
    }

    #[test]
    // Coll[(SInt, SLong)]
    fn render_register_coll_of_tuple_of_int_and_long() {
        let base16_str = "0c400504b40180febe81027880d4d4ab015a80bfdf80013c80aaea55";
        assert_eq!(
            render_register_value(base16_str),
            "[[90,270000000],[60,180000000],[45,135000000],[30,90000000]]"
        );
    }

    #[test]
    // Coll[SLong]
    fn render_register_coll_long() {
        let base16_str = "1106640a8094ebdc0380a0b787e905f4ac46c203";
        assert_eq!(
            render_register_value(base16_str),
            "[50,5,500000000,100000000000,576314,225]"
        );
    }

    #[test]
    // Coll[(SLong, SLong)]
    fn render_register_coll_of_tuple_of_long_and_long() {
        let base16_str = "0c5902c0ea9801c0ddc60190fa9801e0fcc601";
        assert_eq!(
            render_register_value(base16_str),
            "[[1252000,1628000],[1253000,1630000]]"
        );
    }

    #[test]
    // Coll[(SSigmaProp, SLong)]
    fn render_register_coll_of_tuple_of_sigmaprop_and_long() {
        let base16_str = "0c440502cd020ffd8b096232c6753219b6ecc03fa615a6202d1bcf5b4b6a7e91bda2d785181a10cd0255b72ffe27588f75a78b7d4dbabac70d7eaf58b0ad56ca314204ea37d025dbe00c";
        assert_eq!(
            render_register_value(base16_str),
            "[[020ffd8b096232c6753219b6ecc03fa615a6202d1bcf5b4b6a7e91bda2d785181a,8],[0255b72ffe27588f75a78b7d4dbabac70d7eaf58b0ad56ca314204ea37d025dbe0,6]]"
        );
    }

    #[test]
    // SBigInt
    fn render_register_bigint() {
        let base16_str = "061913aaf504e4bc1e62173f87a4378c37b49c8ccff196ce3f0ad2";
        assert_eq!(
            render_register_value(base16_str),
            "CBigInt(123456789012345678901234567890123456789012345678901234567890)"
        );
    }

    #[test]
    // SBoolean
    fn render_register_boolean() {
        let base16_str = "0101";
        assert_eq!(render_register_value(base16_str), "TRUE");
    }

    #[test]
    // SInt
    fn render_register_int() {
        let base16_str = "0400";
        assert_eq!(render_register_value(base16_str), "0");
        let base16_str = "0401";
        assert_eq!(render_register_value(base16_str), "-1");
        let base16_str = "0480d92a";
        assert_eq!(render_register_value(base16_str), "349760");
    }

    #[test]
    // SLong
    fn render_register_long() {
        let base16_str = "05b2e0d204";
        assert_eq!(render_register_value(base16_str), "4872217");
    }

    #[test]
    // SShort
    fn render_register_short() {
        let base16_str = "0322";
        assert_eq!(render_register_value(base16_str), "17");
    }

    #[test]
    // SSigmaProp
    fn render_register_sigmaprop() {
        let base16_str = "08cd0327e65711a59378c59359c3e1d0f7abe906479eccb76094e50fe79d743ccc15e6";
        assert_eq!(
            render_register_value(base16_str),
            "0327e65711a59378c59359c3e1d0f7abe906479eccb76094e50fe79d743ccc15e6"
        );
    }
}
