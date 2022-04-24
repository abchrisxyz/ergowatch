use ergotree_ir::chain::address::Address;
use ergotree_ir::chain::address::AddressEncoder;
use ergotree_ir::chain::address::NetworkPrefix;
use ergotree_ir::ergo_tree::ErgoTree;
use ergotree_ir::mir::constant::Constant;
use ergotree_ir::mir::value::CollKind;
use ergotree_ir::mir::value::NativeColl;
use ergotree_ir::mir::value::Value;
use ergotree_ir::serialization::SigmaSerializable;
use ergotree_ir::sigma_protocol::sigma_boolean::SigmaBoolean;
use ergotree_ir::sigma_protocol::sigma_boolean::SigmaProofOfKnowledgeTree;
use ergotree_ir::types::stype::SType;

use log::warn;

pub(super) fn base16_to_address(base16_str: &str) -> String {
    let tree_bytes = base16::decode(base16_str.as_bytes()).unwrap();
    let tree = ErgoTree::sigma_parse_bytes(&tree_bytes).unwrap();
    let recreated = Address::recreate_from_ergo_tree(&tree).unwrap();
    let encoder = AddressEncoder::new(NetworkPrefix::Mainnet);
    encoder.address_to_str(&recreated)
}

#[derive(Debug)]
pub struct RenderedRegister {
    pub value_type: String,
    pub value: String,
}

pub(super) fn render_register_value(base16_str: &str) -> RenderedRegister {
    let bytes = match base16::decode(base16_str.as_bytes()) {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!("Base16 decoding error: {:?}", err);
            return RenderedRegister {
                value_type: String::from("_Base16DecodeError"),
                value: String::from("_Base16DecodeError"),
            };
        }
    };
    let cst = match Constant::sigma_parse_bytes(&bytes) {
        Ok(cst) => cst,
        Err(err) => {
            warn!("Sigma bytes parsing error: {:?}", err);
            return RenderedRegister {
                value_type: String::from("_SigmaParsingError"),
                value: String::from("_SigmaParsingError"),
            };
        }
    };
    let val = Value::from(cst.v);
    render_register_val(&val)
}

fn render_sbyte(i8: i8) -> String {
    format!("{:02x}", i8 as u8)
}

fn render_register_val(val: &Value) -> RenderedRegister {
    match val {
        Value::Boolean(b) => RenderedRegister {
            value_type: String::from("SBoolean"),
            value: b.to_string().to_uppercase(),
        },
        Value::Byte(i8) => RenderedRegister {
            value_type: String::from("SByte"),
            value: render_sbyte(*i8),
            // value: format!("{:02x}", *i8 as u8),
        },
        Value::Short(i16) => RenderedRegister {
            value_type: String::from("SShort"),
            value: i16.to_string(),
        },
        Value::Int(i32) => RenderedRegister {
            value_type: String::from("SInt"),
            value: i32.to_string(),
        },
        Value::Long(i64) => RenderedRegister {
            value_type: String::from("SLong"),
            value: i64.to_string(),
        },
        Value::BigInt(bi256) => RenderedRegister {
            value_type: String::from("SBigInt"),
            value: format!("CBigInt({})", bi256),
        },
        Value::GroupElement(e) => RenderedRegister {
            value_type: String::from("SGroupElement"),
            value: base16::encode_lower(&e.sigma_serialize_bytes().unwrap()),
        },
        Value::SigmaProp(sp) => {
            let sb = sp.value();
            RenderedRegister {
                value_type: String::from("SSigmaProp"),
                value: match sb {
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
                },
            }
        }
        Value::AvlTree(tree) => RenderedRegister {
            value_type: String::from("SAvlTree"),
            value: base16::encode_lower(&tree.sigma_serialize_bytes().unwrap()),
        },
        Value::CBox(ergo_box) => {
            let eb = ergo_box.as_ref();
            let s = serde_json::to_string(eb).unwrap();
            RenderedRegister {
                value_type: String::from("SCBox"),
                value: s,
            }
        }
        // TODO: don't rely on Debug trait
        Value::Opt(opt) => RenderedRegister {
            value_type: String::from("Opt"),
            value: format!("{:?}", opt),
        },
        Value::Coll(coll) => {
            let raw_values = coll.as_vec();
            // Handle empty collections
            if raw_values.is_empty() {
                let elem_type = match &coll {
                    CollKind::NativeColl(ncoll) => match ncoll {
                        NativeColl::CollByte(_) => &SType::SByte,
                    },
                    CollKind::WrappedColl { elem_tpe, .. } => elem_tpe,
                };
                return RenderedRegister {
                    value_type: format!("Coll[{:?}]", elem_type)
                        // Turn SColl(...) into Coll[...]
                        .replace("SColl", "Coll")
                        .replace("(", "[")
                        .replace(")", "]"),
                    value: match elem_type {
                        SType::SByte => String::from(""),
                        _ => String::from("[]"),
                    },
                };
            }
            let rendered_values: Vec<RenderedRegister> =
                raw_values.iter().map(|v| render_register_val(v)).collect();
            let contains_bytes = match coll {
                CollKind::NativeColl(_) => true,
                CollKind::WrappedColl { elem_tpe, .. } => match elem_tpe {
                    SType::SByte => true,
                    _ => false,
                },
            };
            // Value type
            let vt = match contains_bytes {
                true => "SByte",
                false => &rendered_values.first().unwrap().value_type,
            };
            // Rendered value
            let rv = match contains_bytes {
                true => format!(
                    "{}",
                    rendered_values
                        .iter()
                        .map(|rr| rr.value.clone())
                        .collect::<Vec<String>>()
                        .join("")
                ),
                false => format!(
                    "[{}]",
                    rendered_values
                        .iter()
                        .map(|rr| rr.value.clone())
                        .collect::<Vec<String>>()
                        .join(",")
                ),
            };
            RenderedRegister {
                value_type: format!("Coll[{}]", vt),
                value: rv,
            }
        }
        Value::Tup(items) => {
            let rendered_registers: Vec<RenderedRegister> =
                items.iter().map(|v| render_register_val(v)).collect();
            RenderedRegister {
                value_type: format!(
                    "({})",
                    rendered_registers
                        .iter()
                        .map(|rr| rr.value_type.clone())
                        .collect::<Vec<String>>()
                        .join(", ")
                ),
                value: format!(
                    "[{}]",
                    rendered_registers
                        .iter()
                        .map(|rr| rr.value.clone())
                        .collect::<Vec<String>>()
                        .join(",")
                ),
            }
        }
        // Value comes from a Constant, so remaining Value variants (Context,
        // Header, PreHeader, Global and Lambda) should not occur.
        // If unhandled values were to occur, they can be handled retroactively through migrations.
        _ => RenderedRegister {
            value_type: String::from("unhandled"),
            value: String::from("unhandled"),
        },
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
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SLong");
        assert_eq!(rr.value, "261824656027858");
    }

    #[test]
    fn render_register_value_group_element() {
        let base16_str = "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SGroupElement");
        assert_eq!(
            rr.value,
            "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"
        );
    }

    #[test]
    fn render_register_value_coll_of_byte() {
        let base16_str = "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[SByte]");
        assert_eq!(
            rr.value,
            "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"
        );
    }

    #[test]
    fn render_register_empty_coll_of_byte() {
        let base16_str = "0e00";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[SByte]");
        assert_eq!(rr.value, "");
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
    fn render_register_coll_of_coll_byte() {
        let base16_str = "1a0332537570706f72742074686520726166666c6527732055492f555820646576656c6f706d656e742c206279206d68735f73616da205417320796f752063616e20736565207468652055492f5558206f662074686973207365727669636520697320766572792062617369632e20497420697320626f74686572696e6720626f746820796f7520616e642075732e204c6574277320737461727420746869732073657276696365207769746820796f757220646f6e6174696f6e7320666f722055492f555820646576656c6f706d656e742e0a0a5765206e65656420746f207261697365203135302045524720666f72207468697320707572706f736520616e642074686520726169736564206d6f6e65792077696c6c20626520646f6e6174656420746f20636f6d6d756e697479206465767320746f206372656174652061207375697461626c6520616e64206f70656e20736f757263652055492f555820666f72207468697320736572766963652e200a0a4966207261697365642066756e64732077657265206d6f7265207468616e20313530204552472c207468652065786365737320616d6f756e742077696c6c206265207573656420666f722055492f555820616e642f6f72206f7468657220646576656c6f706d656e74732072656c6174656420746f204572676f20526166666c652e0a0a49276d206d68735f73616d2c204572676f20466f756e646174696f6e20426f617264204d656d62657220616e642074686520666f756e646572206f66204572676f20526166666c652e200a0a546869732066756e6472616973696e6720697320706572736f6e616c20616e6420666f7220746865206d656e74696f6e656420676f616c20616e6420686173206e6f7468696e6720746f20646f2077697468204572676f20466f756e646174696f6e2e0a0a596f752063616e2066696e64206d652061743a2068747470733a2f2f747769747465722e636f6d2f6d68735f73616d201eca1d77eebdb0e9096fdecb6e047ee2169e7c9aef97b0721ad96662f9504bce";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[Coll[SByte]]");
        assert_eq!(
            rr.value,
            "[537570706f72742074686520726166666c6527732055492f555820646576656c6f706d656e742c206279206d68735f73616d,417320796f752063616e20736565207468652055492f5558206f662074686973207365727669636520697320766572792062617369632e20497420697320626f74686572696e6720626f746820796f7520616e642075732e204c6574277320737461727420746869732073657276696365207769746820796f757220646f6e6174696f6e7320666f722055492f555820646576656c6f706d656e742e0a0a5765206e65656420746f207261697365203135302045524720666f72207468697320707572706f736520616e642074686520726169736564206d6f6e65792077696c6c20626520646f6e6174656420746f20636f6d6d756e697479206465767320746f206372656174652061207375697461626c6520616e64206f70656e20736f757263652055492f555820666f72207468697320736572766963652e200a0a4966207261697365642066756e64732077657265206d6f7265207468616e20313530204552472c207468652065786365737320616d6f756e742077696c6c206265207573656420666f722055492f555820616e642f6f72206f7468657220646576656c6f706d656e74732072656c6174656420746f204572676f20526166666c652e0a0a49276d206d68735f73616d2c204572676f20466f756e646174696f6e20426f617264204d656d62657220616e642074686520666f756e646572206f66204572676f20526166666c652e200a0a546869732066756e6472616973696e6720697320706572736f6e616c20616e6420666f7220746865206d656e74696f6e656420676f616c20616e6420686173206e6f7468696e6720746f20646f2077697468204572676f20466f756e646174696f6e2e0a0a596f752063616e2066696e64206d652061743a2068747470733a2f2f747769747465722e636f6d2f6d68735f73616d,1eca1d77eebdb0e9096fdecb6e047ee2169e7c9aef97b0721ad96662f9504bce]"
        );
    }

    #[test]
    fn render_register_empty_coll_of_coll_byte() {
        let base16_str = "1a00";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[Coll[SByte]]");
        assert_eq!(rr.value, "[]");
    }

    #[test]
    fn render_register_coll_of_tuple_of_two_coll_byte() {
        let base16_str = "0c3c0e0e01240008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb7333967556962486f616569774b5a53707967685a4536594d455a564a753977734b7a465332335778525671366e7a547663476f55";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[(Coll[SByte], Coll[SByte])]");
        assert_eq!(
            rr.value,
            "[[0008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb7,3967556962486f616569774b5a53707967685a4536594d455a564a753977734b7a465332335778525671366e7a547663476f55]]"
        );
    }

    #[test]
    fn render_register_coll_of_tuple_of_coll_byte_and_coll_long() {
        let base16_str = "0c3c0e1101240008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb703028084af5f00";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[(Coll[SByte], Coll[SLong])]");
        assert_eq!(
            rr.value,
            "[[0008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb7,[1,100000000,0]]]"
        );
    }

    #[test]
    fn render_register_coll_of_tuple_of_coll_byte_and_coll_int() {
        let base16_str =
            "0c4c0e01240008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb714";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[(Coll[SByte], SInt)]");
        assert_eq!(
            rr.value,
            "[[0008cd0302122c332fd4e3c901f045ac18f559dcecf8dc61f6f94fbb34d0c7c3aac71fb7,10]]"
        );
    }

    #[test]
    fn render_register_tuple_of_coll_byte_and_coll_byte() {
        let base16_str = "3c0e0e42697066733a2f2f62616679626569666d717579686e6f73786e6b7a62666c6e74687775796675767137756e6b3566727374633633327836767662647962676333697542697066733a2f2f6261666b726569657032616a63756165716c767a6776696468743436367a64776a686f7570707773643779707a33737a6778637433367a3661656d";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "(Coll[SByte], Coll[SByte])");
        assert_eq!(rr.value, "[697066733a2f2f62616679626569666d717579686e6f73786e6b7a62666c6e74687775796675767137756e6b35667273746336333278367676626479626763336975,697066733a2f2f6261666b726569657032616a63756165716c767a6776696468743436367a64776a686f7570707773643779707a33737a6778637433367a3661656d]");
    }

    #[test]
    fn render_register_coll_int() {
        let base16_str = "100204a00b";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[SInt]");
        assert_eq!(rr.value, "[2,720]");
    }

    #[test]
    fn render_register_coll_of_tuple_of_int_and_long() {
        let base16_str = "0c400504b40180febe81027880d4d4ab015a80bfdf80013c80aaea55";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[(SInt, SLong)]");
        assert_eq!(
            rr.value,
            "[[90,270000000],[60,180000000],[45,135000000],[30,90000000]]"
        );
    }

    #[test]
    fn render_register_coll_long() {
        let base16_str = "1106640a8094ebdc0380a0b787e905f4ac46c203";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[SLong]");
        assert_eq!(rr.value, "[50,5,500000000,100000000000,576314,225]");
    }

    #[test]
    fn render_register_coll_of_tuple_of_long_and_long() {
        let base16_str = "0c5902c0ea9801c0ddc60190fa9801e0fcc601";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[(SLong, SLong)]");
        assert_eq!(rr.value, "[[1252000,1628000],[1253000,1630000]]");
    }

    #[test]
    fn render_register_coll_of_tuple_of_sigmaprop_and_long() {
        let base16_str = "0c440502cd020ffd8b096232c6753219b6ecc03fa615a6202d1bcf5b4b6a7e91bda2d785181a10cd0255b72ffe27588f75a78b7d4dbabac70d7eaf58b0ad56ca314204ea37d025dbe00c";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "Coll[(SSigmaProp, SLong)]");
        assert_eq!(rr.value, "[[020ffd8b096232c6753219b6ecc03fa615a6202d1bcf5b4b6a7e91bda2d785181a,8],[0255b72ffe27588f75a78b7d4dbabac70d7eaf58b0ad56ca314204ea37d025dbe0,6]]");
    }

    #[test]
    fn render_register_bigint() {
        let base16_str = "061913aaf504e4bc1e62173f87a4378c37b49c8ccff196ce3f0ad2";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SBigInt");
        assert_eq!(
            rr.value,
            "CBigInt(123456789012345678901234567890123456789012345678901234567890)"
        );
    }

    #[test]
    fn render_register_boolean() {
        let base16_str = "0101";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SBoolean");
        assert_eq!(rr.value, "TRUE");
    }

    #[test]
    fn render_register_int() {
        let rr = render_register_value("0400");
        assert_eq!(rr.value_type, "SInt");
        assert_eq!(rr.value, "0");

        let rr = render_register_value("0401");
        assert_eq!(rr.value_type, "SInt");
        assert_eq!(rr.value, "-1");

        let rr = render_register_value("0480d92a");
        assert_eq!(rr.value_type, "SInt");
        assert_eq!(rr.value, "349760");
    }

    #[test]
    fn render_register_long() {
        let base16_str = "05b2e0d204";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SLong");
        assert_eq!(rr.value, "4872217");
    }

    #[test]
    fn render_register_short() {
        let base16_str = "0322";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SShort");
        assert_eq!(rr.value, "17");
    }

    #[test]
    fn render_register_sigmaprop() {
        let base16_str = "08cd0327e65711a59378c59359c3e1d0f7abe906479eccb76094e50fe79d743ccc15e6";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SSigmaProp");
        assert_eq!(
            rr.value,
            "0327e65711a59378c59359c3e1d0f7abe906479eccb76094e50fe79d743ccc15e6"
        );
    }

    #[test]
    fn render_register_avltree() {
        // https://explorer.ergoplatform.com/en/transactions/82018eef5de2df5298cfe7de17bec4ab9c3d1d1596cf7f48091ce114503066b7
        // According to https://github.com/ergoplatform/ergo-jde/blob/main/kiosk/src/test/scala/kiosk/avltree/AvlTrees.md
        // R4 and R5 are of type AvlTree
        //
        // "additionalRegisters": {
        //     "R4": "64aebde47e15b6bfb577265ea5a819f5779328085286d86e7e1089636641dae9b80007200108",
        //     "R5": "64da041b2cfe44c3e34bcf0accd22dd7c52d2c278bef80587ab3d4e49b5dba86c10107200108",
        //     "R6": "0e200a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a",
        //     "R7": "0e081414141414141414",
        //     "R8": "0e4a020000000000000000000000000000000000000000000000000000000000000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000000000000000004"
        //   },
        //   "transactionId": "82018eef5de2df5298cfe7de17bec4ab9c3d1d1596cf7f48091ce114503066b7",
        //
        let base16_str =
            "64aebde47e15b6bfb577265ea5a819f5779328085286d86e7e1089636641dae9b80007200108";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SAvlTree");
        assert_eq!(
            rr.value,
            "aebde47e15b6bfb577265ea5a819f5779328085286d86e7e1089636641dae9b80007200108"
        );
    }

    // Second output of this tx fe61c57a2a360bfd4bebe8f3e5702fdb42a709d822ad88dcc81606a7be94360e
    // has a CBox in R4.
    // Explorer doesn't seem to handle CBox registers (yet), so use node api to look up the tx.
    // It is part of block ca5631c4ab3579977cccd346c33dd032c5d3fbd165b7000156f43f3e61dd67f1.
    #[test]
    fn render_register_cbox() {
        let base16_str = "6380e3be0a100d0400040004000500040405020580b489130e8702101404000e240008cd02df65e86fbc84e0534a9170de4cfcf3c31f28367c07ef68f38d2b346f752ec49204000400040004000580dac40904020580dac409050005c083ccb2f95e050004010101010004020e000500058092f4010404d803d601b2a5730000d602db63087201d6037301d1ecededededededed91b17202730293e5c67201040ec5b2a4730300720393e5c67201050ec5b2a4730400720393b2e4c672010611730500730693b2e4c672010611730700730893e5c6720107057309730a93e5c672010805730b7e730c05ec730ded730e938cb27202730f00017310eded92c1720199b0a47311d9010441639a8c720401c18c720402731293c27201720393b1a573130e0201010e20c72af9dd2e78700de6b55ea472025e9f9eaadd602ec4f4bb64b7ae4927ceed5805000580a4e8030e240008cd02d84d2e1ca735ce23f224bd43cd43ed67053356713a8a6920965b3bde933648dcd804d601b2a5730000d602c17201d603c5b2a4730100d604b2db63087201730201860272037303d1ed93b1a57304ecededededed9372038c720401938c7204027305937202730693c27201730793e4c67201070e730893e4c67201080e7309ed92720299b0a4730ad9010541639a8c720501c18c720502730b93c27201730cfcfc220000fa12ef1163fa3bd6ac7178f344a6ed3a3bacb1a0ccf1eab1cc74ebf0d8583dc200";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "SCBox");
        assert_eq!(rr.value, "{\"boxId\":\"1010ff69fabe02798af4765600fef9dd3983ff5aacb717d2aaa642eb1e129c57\",\"value\":22000000,\"ergoTree\":\"100d0400040004000500040405020580b489130e8702101404000e240008cd02df65e86fbc84e0534a9170de4cfcf3c31f28367c07ef68f38d2b346f752ec49204000400040004000580dac40904020580dac409050005c083ccb2f95e050004010101010004020e000500058092f4010404d803d601b2a5730000d602db63087201d6037301d1ecededededededed91b17202730293e5c67201040ec5b2a4730300720393e5c67201050ec5b2a4730400720393b2e4c672010611730500730693b2e4c672010611730700730893e5c6720107057309730a93e5c672010805730b7e730c05ec730ded730e938cb27202730f00017310eded92c1720199b0a47311d9010441639a8c720401c18c720402731293c27201720393b1a573130e0201010e20c72af9dd2e78700de6b55ea472025e9f9eaadd602ec4f4bb64b7ae4927ceed5805000580a4e8030e240008cd02d84d2e1ca735ce23f224bd43cd43ed67053356713a8a6920965b3bde933648dcd804d601b2a5730000d602c17201d603c5b2a4730100d604b2db63087201730201860272037303d1ed93b1a57304ecededededed9372038c720401938c7204027305937202730693c27201730793e4c67201070e730893e4c67201080e7309ed92720299b0a4730ad9010541639a8c720501c18c720502730b93c27201730c\",\"assets\":[],\"additionalRegisters\":{},\"creationHeight\":573052,\"transactionId\":\"fa12ef1163fa3bd6ac7178f344a6ed3a3bacb1a0ccf1eab1cc74ebf0d8583dc2\",\"index\":0}");
    }

    #[test]
    fn render_register_error_handling() {
        // Invalid ByteCode
        let base16_str = "62";
        let rr = render_register_value(base16_str);
        assert_eq!(rr.value_type, "_SigmaParsingError");
        assert_eq!(rr.value, "_SigmaParsingError");
    }
}
