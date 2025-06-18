plan {
  opr {
    scan {
      alias {
      }
      params {
        tables {
          id: 0
        }
        predicate {
          operators {
            const {
              i32: 50
            }
            node_type {
              data_type {
                primitive_type: DT_SIGNED_INT32
              }
            }
          }
          operators {
            logical: LT
            node_type {
              data_type {
                primitive_type: DT_BOOL
              }
            }
          }
          operators {
            var {
              property {
                key {
                  name: "id"
                }
              }
              node_type {
                data_type {
                  primitive_type: DT_SIGNED_INT64
                }
              }
            }
            node_type {
              data_type {
                primitive_type: DT_SIGNED_INT64
              }
            }
          }
          operators {
            logical: AND
            node_type {
              data_type {
                primitive_type: DT_BOOL
              }
            }
          }
          operators {
            var {
              property {
                key {
                  name: "id"
                }
              }
              node_type {
                data_type {
                  primitive_type: DT_SIGNED_INT64
                }
              }
            }
            node_type {
              data_type {
                primitive_type: DT_SIGNED_INT64
              }
            }
          }
          operators {
            logical: LT
            node_type {
              data_type {
                primitive_type: DT_BOOL
              }
            }
          }
          operators {
            const {
              i32: 100
            }
            node_type {
              data_type {
                primitive_type: DT_SIGNED_INT32
              }
            }
          }
        }
        sample_ratio: 1
      }
    }
  }
  meta_data {
    type {
      graph_type {
        graph_data_type {
          label {
          }
          props {
            prop_id {
              name: "id"
            }
            type {
              primitive_type: DT_SIGNED_INT64
            }
          }
          props {
            prop_id {
              name: "name"
            }
            type {
              string {
                long_text {
                }
              }
            }
          }
          props {
            prop_id {
              name: "age"
            }
            type {
              primitive_type: DT_SIGNED_INT32
            }
          }
        }
      }
    }
  }
}
plan {
  opr {
    edge {
      v_tag {
      }
      direction: BOTH
      params {
        tables {
          id: 1
        }
        columns {
          name: "weight"
        }
        sample_ratio: 1
      }
      expand_opt: EDGE
    }
  }
  meta_data {
    type {
      graph_type {
        element_opt: EDGE
        graph_data_type {
          label {
            label: 1
            src_label {
            }
            dst_label {
              value: 1
            }
          }
          props {
            prop_id {
              name: "weight"
            }
            type {
              primitive_type: DT_DOUBLE
            }
          }
        }
      }
    }
    alias: -1
  }
}
plan {
  opr {
    vertex {
      opt: OTHER
      params {
        tables {
          id: 1
        }
        sample_ratio: 1
      }
      alias {
        value: 1
      }
    }
  }
  meta_data {
    type {
      graph_type {
        graph_data_type {
          label {
            label: 1
          }
          props {
            prop_id {
              name: "id"
            }
            type {
              primitive_type: DT_SIGNED_INT64
            }
          }
          props {
            prop_id {
              name: "name"
            }
            type {
              string {
                long_text {
                }
              }
            }
          }
          props {
            prop_id {
              name: "lang"
            }
            type {
              string {
                long_text {
                }
              }
            }
          }
        }
      }
    }
    alias: 1
  }
}
plan {
  opr {
    project {
      mappings {
        expr {
          operators {
            var {
              tag {
                id: 0
              }
              property {
                key {
                  name: "name"
                }
              }
              node_type {
                data_type {
                  string {
                    long_text {
                    }
                  }
                }
              }
            }
            node_type {
              data_type {
                string {
                  long_text {
                  }
                }
              }
            }
          }
        }
        alias {
          value: 2
        }
      }
      mappings {
        expr {
          operators {
            var {
              tag {
                id: 1
              }
              property {
                key {
                  name: "name"
                }
              }
              node_type {
                data_type {
                  string {
                    long_text {
                    }
                  }
                }
              }
            }
            node_type {
              data_type {
                string {
                  long_text {
                  }
                }
              }
            }
          }
        }
        alias {
          value: 3
        }
      }
    }
  }
  meta_data {
    type {
      data_type {
        string {
          long_text {
          }
        }
      }
    }
    alias: 2
  }
  meta_data {
    type {
      data_type {
        string {
          long_text {
          }
        }
      }
    }
    alias: 3
  }
}
plan {
  opr {
    sink {
      tags {
        tag {
          value: 2
        }
      }
      tags {
        tag {
          value: 3
        }
      }
      sink_target {
        sink_default {
        }
      }
    }
  }
}