#include "katana/RDG.h"

#include <cassert>
#include <exception>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <unordered_set>
#include <vector>

#include <arrow/chunked_array.h>
#include <arrow/filesystem/api.h>
#include <arrow/memory_pool.h>
#include <arrow/type_fwd.h>
#include <arrow/util/string_view.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/platform.h>
#include <parquet/properties.h>

#include "AddProperties.h"
#include "GlobalState.h"
#include "RDGCore.h"
#include "RDGHandleImpl.h"
#include "katana/ArrowInterchange.h"
#include "katana/ErrorCode.h"
#include "katana/FaultTest.h"
#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/ParquetWriter.h"
#include "katana/RDGTopology.h"
#include "katana/ReadGroup.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/WriteGroup.h"
#include "katana/file.h"
#include "katana/tsuba.h"

using json = nlohmann::json;

namespace {

katana::Result<std::string>
StoreArrowArrayAtName(
    const std::shared_ptr<arrow::ChunkedArray>& array, const katana::Uri& dir,
    const std::string& name, katana::WriteGroup* desc) {
  std::unique_ptr<katana::ParquetWriter> writer =
      KATANA_CHECKED(katana::ParquetWriter::Make(array, name));

  katana::Uri new_path = dir.RandFile(name);
  KATANA_CHECKED_CONTEXT(
      writer->WriteToUri(new_path, desc), "writing to: {}", new_path);
  return new_path.BaseName();
}

katana::Result<void>
WriteProperties(
    const arrow::Table& props, std::vector<katana::PropStorageInfo*> prop_info,
    const katana::Uri& dir, katana::WriteGroup* desc) {
  const auto& schema = props.schema();

  std::vector<std::string> next_paths;
  for (size_t i = 0, n = prop_info.size(); i < n; ++i) {
    if (!prop_info[i]->IsDirty()) {
      continue;
    }
    std::string name = prop_info[i]->name().empty() ? schema->field(i)->name()
                                                    : prop_info[i]->name();
    std::string path =
        KATANA_CHECKED(StoreArrowArrayAtName(props.column(i), dir, name, desc));

    prop_info[i]->WasWritten(path);
  }
  TSUBA_PTP(katana::internal::FaultSensitivity::Normal);

  return katana::ResultSuccess();
}

katana::Result<void>
CommitRDG(
    katana::RDGHandle handle, uint32_t policy_id, bool transposed,
    katana::RDG::RDGVersioningPolicy versioning_action,
    const katana::RDGLineage& lineage,
    std::unique_ptr<katana::WriteGroup> desc) {
  katana::CommBackend* comm = katana::Comm();
  katana::RDGManifest new_manifest =
      (versioning_action == katana::RDG::RetainVersion)
          ? handle.impl_->rdg_manifest().SameVersion(
                comm->Num, policy_id, transposed, lineage)
          : handle.impl_->rdg_manifest().NextVersion(
                comm->Num, policy_id, transposed, lineage);

  // wait for all the work we queued to finish
  TSUBA_PTP(katana::internal::FaultSensitivity::High);
  KATANA_CHECKED_CONTEXT(desc->Finish(), "at least one async write failed");

  TSUBA_PTP(katana::internal::FaultSensitivity::High);
  comm->Barrier();

  TSUBA_PTP(katana::internal::FaultSensitivity::High);
  katana::Result<void> ret = katana::OneHostOnly([&]() -> katana::Result<void> {
    TSUBA_PTP(katana::internal::FaultSensitivity::High);

    std::string curr_s = new_manifest.ToJsonString();
    auto manifest_file = katana::RDGManifest::FileName(
        handle.impl_->rdg_manifest().dir(),
        handle.impl_->rdg_manifest().viewtype(), new_manifest.version());
    KATANA_CHECKED_CONTEXT(
        katana::FileStore(
            manifest_file.string(),
            reinterpret_cast<const uint8_t*>(curr_s.data()), curr_s.size()),
        "CommitRDG future failed {}", manifest_file);
    return katana::ResultSuccess();
  });
  if (ret) {
    handle.impl_->set_rdg_manifest(std::move(new_manifest));
  }
  return ret;
}

}  // namespace

void
katana::RDG::AddLineage(const std::string& command_line) {
  core_->AddCommandLine(command_line);
}

katana::RDGFile::~RDGFile() {
  auto result = Close(handle_);
  if (!result) {
    KATANA_LOG_ERROR("closing RDGFile: {}", result.error());
  }
}

katana::Result<std::vector<katana::PropStorageInfo>>
katana::RDG::WritePartArrays(const katana::Uri& dir, katana::WriteGroup* desc) {
  std::vector<katana::PropStorageInfo> next_properties;

  KATANA_LOG_DEBUG(
      "WritePartArrays master sz: {} mirrors sz: {} h2owned sz : {} "
      "h2owned_edges sz: {} l2u sz: {} "
      "l2g sz: {}",
      master_nodes().size(), mirror_nodes().size(),
      host_to_owned_global_node_ids() == nullptr
          ? 0
          : host_to_owned_global_node_ids()->length(),
      host_to_owned_global_edge_ids() == nullptr
          ? 0
          : host_to_owned_global_edge_ids()->length(),
      local_to_user_id() == nullptr ? 0 : local_to_user_id()->length(),
      local_to_global_id() == nullptr ? 0 : local_to_global_id()->length());

  for (size_t i = 0; i < mirror_nodes().size(); ++i) {
    std::string name = RDGCore::MirrorPropName(i);
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(mirror_nodes()[i], dir, name, desc), "storing {}",
        name);
    next_properties.emplace_back(katana::PropStorageInfo(name, path));
  }

  for (size_t i = 0; i < master_nodes().size(); ++i) {
    std::string name = RDGCore::MasterPropName(i);
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(master_nodes()[i], dir, name, desc), "storing {}",
        name);
    next_properties.emplace_back(katana::PropStorageInfo(name, path));
  }

  if (host_to_owned_global_node_ids() != nullptr) {
    std::string name = RDGCore::kHostToOwnedGlobalNodeIDsPropName;
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(host_to_owned_global_node_ids(), dir, name, desc),
        "storing {}", name);
    next_properties.emplace_back(katana::PropStorageInfo(name, path));
  }

  if (host_to_owned_global_edge_ids() != nullptr) {
    std::string name = RDGCore::kHostToOwnedGlobalEdgeIDsPropName;
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(host_to_owned_global_edge_ids(), dir, name, desc),
        "storing {}", name);
    next_properties.emplace_back(katana::PropStorageInfo(name, path));
  }

  if (local_to_user_id() != nullptr) {
    std::string name = RDGCore::kLocalToUserIDPropName;
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(local_to_user_id(), dir, name, desc),
        "storing {}", name);
    next_properties.emplace_back(katana::PropStorageInfo(name, path));
  }

  if (local_to_global_id() != nullptr) {
    std::string name = RDGCore::kLocalToGlobalIDPropName;
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(local_to_global_id(), dir, name, desc),
        "storing {}", name);
    next_properties.emplace_back(katana::PropStorageInfo(name, path));
  }

  return next_properties;
}

//TODO : emcginnis combine the Edge and Node DoStoreNode/EntityTypeIDArray
// into a single generalized function.
katana::Result<void>
katana::RDG::DoStoreNodeEntityTypeIDArray(
    RDGHandle handle, std::unique_ptr<FileFrame> node_entity_type_id_array_ff,
    std::unique_ptr<WriteGroup>& write_group) {
  if (!node_entity_type_id_array_ff &&
      !node_entity_type_id_array_file_storage().Valid()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "no node_entity_type_id_array file frame update, but "
        "node_entity_type_id_array_file_storage is invalid");
  }

  if (node_entity_type_id_array_ff) {
    // we have an update, store the passed in memory state
    katana::Uri path_uri = MakeNodeEntityTypeIDArrayFileName(handle);
    node_entity_type_id_array_ff->Bind(path_uri.string());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    write_group->StartStore(std::move(node_entity_type_id_array_ff));
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_node_entity_type_id_array_path(
        path_uri.BaseName());
  } else if (handle.impl_->rdg_manifest().dir() != rdg_dir()) {
    KATANA_LOG_DEBUG("persisting node_entity_type_id_array in new location");
    // we don't have an update, but we are persisting in a new location
    // store our in memory state
    katana::Uri path_uri = MakeNodeEntityTypeIDArrayFileName(handle);

    TSUBA_PTP(internal::FaultSensitivity::Normal);
    // depends on `node_entity_type_id_array_` outliving writes
    write_group->StartStore(
        path_uri.string(),
        core_->node_entity_type_id_array_file_storage().ptr<uint8_t>(),
        core_->node_entity_type_id_array_file_storage().size());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_node_entity_type_id_array_path(
        path_uri.BaseName());
  } else {
    // no update, rdg_dir is unchanged, assert if we don't have a valid path
    KATANA_LOG_ASSERT(
        core_->part_header().node_entity_type_id_array_path().empty() == false);
  }
  // else: no update, not persisting in a new location, so nothing for us to do

  return katana::ResultSuccess();
}

//TODO : emcginnis combine the Edge and Node DoStoreNode/EntityTypeIDArray
// into a single generalized function.
katana::Result<void>
katana::RDG::DoStoreEdgeEntityTypeIDArray(
    RDGHandle handle, std::unique_ptr<FileFrame> edge_entity_type_id_array_ff,
    std::unique_ptr<WriteGroup>& write_group) {
  if (!edge_entity_type_id_array_ff &&
      !edge_entity_type_id_array_file_storage().Valid()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "no edge_entity_type_id_array file frame update, but "
        "edge_entity_type_id_array_file_storage is invalid");
  }

  if (edge_entity_type_id_array_ff) {
    // we have an update, store the passed in memory state
    katana::Uri path_uri = MakeEdgeEntityTypeIDArrayFileName(handle);
    edge_entity_type_id_array_ff->Bind(path_uri.string());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    write_group->StartStore(std::move(edge_entity_type_id_array_ff));
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_edge_entity_type_id_array_path(
        path_uri.BaseName());
  } else if (handle.impl_->rdg_manifest().dir() != rdg_dir()) {
    KATANA_LOG_DEBUG("persisting edge_entity_type_id_array in new location");
    // we don't have an update, but we are persisting in a new location
    // store our in memory state
    katana::Uri path_uri = MakeEdgeEntityTypeIDArrayFileName(handle);

    TSUBA_PTP(internal::FaultSensitivity::Normal);
    // depends on `edge_entity_type_id_array_` outliving writes
    write_group->StartStore(
        path_uri.string(),
        core_->edge_entity_type_id_array_file_storage().ptr<uint8_t>(),
        core_->edge_entity_type_id_array_file_storage().size());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_edge_entity_type_id_array_path(
        path_uri.BaseName());
  } else {
    // no update, rdg_dir is unchanged, assert if we don't have a valid path
    KATANA_LOG_ASSERT(
        core_->part_header().edge_entity_type_id_array_path().empty() == false);
  }
  // else: no update, not persisting in a new location, so nothing for us to do

  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDG::DoStore(
    RDGHandle handle, const std::string& command_line,
    RDGVersioningPolicy versioning_action,
    std::unique_ptr<WriteGroup> write_group) {
  // bump the storage format version to the latest
  core_->part_header().update_storage_format_version();

  // all rdgs stored while the unstable rdg storage format flag is set
  // are considered to be in the unstable rdg storage format
  if (KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat)) {
    core_->part_header().set_unstable_storage_format();
  }

  std::vector<std::string> node_prop_names;
  for (const auto& field : core_->node_properties()->fields()) {
    node_prop_names.emplace_back(field->name());
  }

  std::vector<PropStorageInfo*> node_props_to_store = KATANA_CHECKED(
      core_->part_header().SelectNodeProperties(node_prop_names));

  // writing node properties
  KATANA_CHECKED(WriteProperties(
      *core_->node_properties(), node_props_to_store,
      handle.impl_->rdg_manifest().dir(), write_group.get()));

  std::vector<std::string> edge_prop_names;
  for (const auto& field : core_->edge_properties()->fields()) {
    edge_prop_names.emplace_back(field->name());
  }

  std::vector<PropStorageInfo*> edge_props_to_store = KATANA_CHECKED(
      core_->part_header().SelectEdgeProperties(edge_prop_names));

  // writing edge properties
  KATANA_CHECKED(WriteProperties(
      *core_->edge_properties(), edge_props_to_store,
      handle.impl_->rdg_manifest().dir(), write_group.get()));

  // writing partition metadata
  core_->part_header().set_part_prop_info_list(KATANA_CHECKED(
      WritePartArrays(handle.impl_->rdg_manifest().dir(), write_group.get())));

  //If a view type has been set, use it otherwise pass in the default view type
  if (view_type_.empty()) {
    handle.impl_->set_viewtype(katana::kDefaultRDGViewType);
  } else {
    handle.impl_->set_viewtype(view_type_);
  }

  // writing metadata
  KATANA_CHECKED(
      core_->part_header().Write(handle, write_group.get(), versioning_action));

  // Update lineage and commit
  core_->AddCommandLine(command_line);
  KATANA_CHECKED(CommitRDG(
      handle, core_->part_header().metadata().policy_id_,
      core_->part_header().metadata().transposed_, versioning_action,
      core_->lineage(), std::move(write_group)));
  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDG::DoMake(
    const std::vector<PropStorageInfo*>& node_props_to_be_loaded,
    const std::vector<PropStorageInfo*>& edge_props_to_be_loaded,
    const katana::Uri& metadata_dir) {
  ReadGroup grp;

  // populating node properties
  KATANA_CHECKED(AddProperties(
      metadata_dir, true /*is_property*/, node_props_to_be_loaded, &grp,
      [rdg = this](
          const std::shared_ptr<arrow::Table>& props) -> katana::Result<void> {
        std::shared_ptr<arrow::Table> prop_table =
            rdg->core_->node_properties();

        if (prop_table && prop_table->num_columns() > 0) {
          for (int i = 0; i < props->num_columns(); ++i) {
            prop_table = KATANA_CHECKED(prop_table->AddColumn(
                prop_table->num_columns(), props->field(i), props->column(i)));
          }
        } else {
          prop_table = props;
        }
        rdg->core_->set_node_properties(std::move(prop_table));
        return katana::ResultSuccess();
      }));

  // populating edge properties
  KATANA_CHECKED(AddProperties(
      metadata_dir, true /*is_property*/, edge_props_to_be_loaded, &grp,
      [rdg = this](
          const std::shared_ptr<arrow::Table>& props) -> katana::Result<void> {
        std::shared_ptr<arrow::Table> prop_table =
            rdg->core_->edge_properties();

        if (prop_table && prop_table->num_columns() > 0) {
          for (int i = 0; i < props->num_columns(); ++i) {
            prop_table = KATANA_CHECKED(prop_table->AddColumn(
                prop_table->num_columns(), props->field(i), props->column(i)));
          }
        } else {
          prop_table = props;
        }
        rdg->core_->set_edge_properties(std::move(prop_table));
        return katana::ResultSuccess();
      }));

  // populating topologies
  KATANA_CHECKED(core_->MakeTopologyManager(metadata_dir));

  // ensure we can find the default csr topology
  RDGTopology shadow_csr = RDGTopology::MakeShadowCSR();
  RDGTopology* csr = KATANA_CHECKED_CONTEXT(
      core_->topology_manager().GetTopology(shadow_csr),
      "unable to find csr topology, must have csr topology");
  KATANA_LOG_VASSERT(csr != nullptr, "csr topology is null");

  if (core_->part_header().IsEntityTypeIDsOutsideProperties()) {
    katana::Uri node_entity_type_id_array_path = metadata_dir.Join(
        core_->part_header().node_entity_type_id_array_path());
    KATANA_CHECKED(core_->node_entity_type_id_array_file_storage().Bind(
        node_entity_type_id_array_path.string(), true));

    katana::Uri edge_entity_type_id_array_path = metadata_dir.Join(
        core_->part_header().edge_entity_type_id_array_path());
    KATANA_CHECKED(core_->edge_entity_type_id_array_file_storage().Bind(
        edge_entity_type_id_array_path.string(), true));
  }
  core_->set_rdg_dir(metadata_dir);

  std::vector<PropStorageInfo*> part_info =
      KATANA_CHECKED(core_->part_header().SelectPartitionProperties());

  // these are not Node/Edge types but rather property types we are checking
  KATANA_CHECKED(core_->EnsureNodeTypesLoaded());
  KATANA_CHECKED(core_->EnsureEdgeTypesLoaded());

  if (part_info.empty()) {
    return grp.Finish();
  }

  // populating partition metadata
  KATANA_CHECKED(AddProperties(
      metadata_dir, false /*is_property*/, part_info, &grp,
      [rdg = this](const std::shared_ptr<arrow::Table>& props) {
        return rdg->core_->AddPartitionMetadataArray(props);
      }));
  KATANA_CHECKED(grp.Finish());

  if (local_to_user_id()->length() == 0) {
    // for backward compatibility
    if (local_to_global_id()->length() !=
        core_->part_header().metadata().num_nodes_) {
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument,
          "regenerate partitions: number of Global Node IDs {} does not "
          "match the number of master nodes {}",
          local_to_global_id()->length(),
          core_->part_header().metadata().num_nodes_);
    }
    // NB: this is a zero-copy slice, so the underlying data is shared
    core_->set_local_to_user_id(local_to_global_id()->Slice(0));
  } else if (
      local_to_user_id()->length() !=
      (core_->part_header().metadata().num_owned_ +
       local_to_global_id()->length())) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument,
        "regenerate partitions: number of User Node IDs {} do not match "
        "number of masters nodes {} plus the number of Global Node IDs {}",
        local_to_user_id()->length(),
        core_->part_header().metadata().num_owned_,
        local_to_global_id()->length());
  }

  KATANA_LOG_DEBUG(
      "ReadPartMetadata master sz: {} mirrors sz: {} h2nod sz: {} h20e sz: {} "
      "l2u sz: "
      "{} l2g sz: {}",
      master_nodes().size(), mirror_nodes().size(),
      host_to_owned_global_node_ids() == nullptr
          ? 0
          : host_to_owned_global_node_ids()->length(),
      host_to_owned_global_edge_ids() == nullptr
          ? 0
          : host_to_owned_global_edge_ids()->length(),
      local_to_user_id() == nullptr ? 0 : local_to_user_id()->length(),
      local_to_global_id() == nullptr ? 0 : local_to_global_id()->length());

  return katana::ResultSuccess();
}

katana::Result<katana::RDG>
katana::RDG::Make(const RDGManifest& manifest, const RDGLoadOptions& opts) {
  uint32_t partition_id_to_load =
      opts.partition_id_to_load.value_or(Comm()->Rank);

  katana::Uri partition_path = manifest.PartitionFileName(partition_id_to_load);

  RDGPartHeader part_header = KATANA_CHECKED_CONTEXT(
      RDGPartHeader::Make(partition_path), "failed to read path {}",
      partition_path);

  RDG rdg(std::make_unique<RDGCore>(std::move(part_header)));
  // rdg.DoMake will try to lookup in the property cache, and it
  // needs a valid rdg_dir
  rdg.set_rdg_dir(manifest.dir());
  KATANA_LOG_ASSERT(!manifest.dir().empty());

  std::vector<PropStorageInfo*> node_props = KATANA_CHECKED(
      rdg.core_->part_header().SelectNodeProperties(opts.node_properties));

  std::vector<PropStorageInfo*> edge_props = KATANA_CHECKED(
      rdg.core_->part_header().SelectEdgeProperties(opts.edge_properties));

  KATANA_CHECKED(rdg.DoMake(node_props, edge_props, manifest.dir()));

  rdg.core_->set_partition_id(partition_id_to_load);

  return RDG(std::move(rdg));
}

bool
katana::RDG::IsEntityTypeIDsOutsideProperties() const {
  return core_->part_header().IsEntityTypeIDsOutsideProperties();
}

bool
katana::RDG::IsUint16tEntityTypeIDs() const {
  return core_->part_header().IsUint16tEntityTypeIDs();
}

bool
katana::RDG::IsHeaderlessEntityTypeIDArray() const {
  return core_->part_header().IsHeaderlessEntityTypeIDArray();
}

bool
katana::RDG::IsUnstableStorageFormat() const {
  return core_->part_header().unstable_storage_format();
}

katana::Result<void>
katana::RDG::Validate() const {
  KATANA_CHECKED(core_->part_header().Validate());
  return katana::ResultSuccess();
}

bool
katana::RDG::Equals(const RDG& other) const {
  return core_->Equals(*other.core_);
}

katana::Result<katana::RDG>
katana::RDG::Make(RDGHandle handle, const RDGLoadOptions& opts) {
  if (!handle.impl_->AllowsRead()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "handle does not allow full read");
  }
  return Make(handle.impl_->rdg_manifest(), opts);
}

uint64_t
katana::RDG::CurrentVersion(RDGHandle handle) {
  return handle.impl_->rdg_manifest().version();
}

katana::Result<void>
katana::RDG::Store(
    RDGHandle handle, const std::string& command_line,
    RDGVersioningPolicy versioning_action,
    std::unique_ptr<FileFrame> node_entity_type_id_array_ff,
    std::unique_ptr<FileFrame> edge_entity_type_id_array_ff,
    const katana::EntityTypeManager& node_entity_type_manager,
    const katana::EntityTypeManager& edge_entity_type_manager) {
  if (!handle.impl_->AllowsWrite()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "handle does not allow write");
  }
  // We trust the partitioner to give us a valid graph, but we
  // report our assumptions
  KATANA_LOG_DEBUG(
      "RDG::Store manifest.num_hosts: {} manifest.policy_id: {} num_hosts: {} "
      "policy_id: {} versioning_action{}",
      handle.impl_->rdg_manifest().num_hosts(),
      handle.impl_->rdg_manifest().policy_id(), katana::Comm()->Num,
      core_->part_header().metadata().policy_id_, versioning_action);
  if (handle.impl_->rdg_manifest().dir() != rdg_dir()) {
    KATANA_CHECKED(core_->part_header().ChangeStorageLocation(
        rdg_dir(), handle.impl_->rdg_manifest().dir()));
  }

  // All write buffers must outlive desc
  std::unique_ptr<WriteGroup> desc = KATANA_CHECKED(WriteGroup::Make());

  KATANA_CHECKED(core_->topology_manager().DoStore(handle, rdg_dir(), desc));

  KATANA_CHECKED(DoStoreNodeEntityTypeIDArray(
      handle, std::move(node_entity_type_id_array_ff), desc));

  KATANA_CHECKED(DoStoreEdgeEntityTypeIDArray(
      handle, std::move(edge_entity_type_id_array_ff), desc));

  core_->part_header().StoreNodeEntityTypeManager(node_entity_type_manager);
  core_->part_header().StoreEdgeEntityTypeManager(edge_entity_type_manager);

  return DoStore(handle, command_line, versioning_action, std::move(desc));
}

katana::Result<void>
katana::RDG::AddNodeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  return core_->AddNodeProperties(props, txn_ctx);
}

katana::Result<void>
katana::RDG::AddEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  return core_->AddEdgeProperties(props, txn_ctx);
}

katana::Result<void>
katana::RDG::UpsertNodeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  return core_->UpsertNodeProperties(props, txn_ctx);
}

katana::Result<void>
katana::RDG::UpsertEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  return core_->UpsertEdgeProperties(props, txn_ctx);
}

katana::Result<void>
katana::RDG::RemoveNodeProperty(int i, katana::TxnContext* txn_ctx) {
  return core_->RemoveNodeProperty(i, txn_ctx);
}

katana::Result<void>
katana::RDG::RemoveEdgeProperty(int i, katana::TxnContext* txn_ctx) {
  return core_->RemoveEdgeProperty(i, txn_ctx);
}

void
katana::RDG::UpsertTopology(katana::RDGTopology topo) {
  core_->UpsertTopology(std::move(topo));
}

void
katana::RDG::AddTopology(katana::RDGTopology topo) {
  core_->AddTopology(std::move(topo));
}

namespace {

katana::Result<std::shared_ptr<arrow::Table>>
UnloadProperty(
    const std::shared_ptr<arrow::Table>& props, int i,
    std::vector<katana::PropStorageInfo>* prop_info_list,
    const katana::Uri& dir) {
  if (i < 0 || i > props->num_columns()) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument, "property index out of bounds");
  }
  const std::string& name = props->field(i)->name();

  auto psi_it = std::find_if(
      prop_info_list->begin(), prop_info_list->end(),
      [&](const katana::PropStorageInfo& psi) { return psi.name() == name; });

  KATANA_LOG_ASSERT(psi_it != prop_info_list->end());

  katana::PropStorageInfo& prop_info = *psi_it;

  KATANA_LOG_ASSERT(!prop_info.IsAbsent());

  if (prop_info.IsDirty()) {
    std::string path = KATANA_CHECKED(
        StoreArrowArrayAtName(props->column(i), dir, name, nullptr));
    prop_info.WasWritten(path);
  }

  prop_info.WasUnloaded();

  return KATANA_CHECKED(props->RemoveColumn(i));
}

katana::Result<katana::Uri>
GetStorageLocationIfValid(
    const std::string& name,
    const std::vector<katana::PropStorageInfo>& prop_info_list) {
  auto psi_it = std::find_if(
      prop_info_list.begin(), prop_info_list.end(),
      [&](const katana::PropStorageInfo& psi) { return psi.name() == name; });
  if (psi_it == prop_info_list.end()) {
    return KATANA_ERROR(
        katana::ErrorCode::PropertyNotFound, "no property named {}",
        std::quoted(name));
  }

  if (!(psi_it->IsAbsent() || psi_it->IsClean())) {
    return KATANA_ERROR(
        katana::ErrorCode::AssertionFailed, "the property exists but is dirty");
  }
  // TODO(thunt) there's really no reason why we shouldn't always use uri
  auto path = KATANA_CHECKED(katana::Uri::Make(psi_it->path()));
  return path;
}

katana::Result<std::shared_ptr<arrow::Table>>
LoadProperty(
    const std::shared_ptr<arrow::Table>& props, const std::string name, int i,
    std::vector<katana::PropStorageInfo>* prop_info_list,
    const katana::Uri& dir) {
  auto psi_it = std::find_if(
      prop_info_list->begin(), prop_info_list->end(),
      [&](const katana::PropStorageInfo& psi) { return psi.name() == name; });

  if (psi_it == prop_info_list->end()) {
    return KATANA_ERROR(
        katana::ErrorCode::PropertyNotFound, "no property named {}",
        std::quoted(name));
  }

  katana::PropStorageInfo& prop_info = *psi_it;

  if (!prop_info.IsAbsent()) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument, "property {} already loaded",
        std::quoted(name));
  }

  std::shared_ptr<arrow::Table> new_table;

  KATANA_CHECKED(katana::AddProperties(
      dir, true /*is_property*/, {&prop_info}, nullptr,
      [i, &props, &new_table](
          const std::shared_ptr<arrow::Table>& col) -> katana::Result<void> {
        int idx = i;
        if (props->num_columns() > 0) {
          if (idx < 0 || idx > props->num_columns()) {
            idx = props->num_columns();
          }
          new_table = KATANA_CHECKED(
              props->AddColumn(idx, col->field(0), col->column(0)));
        } else {
          new_table = col;
        }
        return katana::ResultSuccess();
      }));

  KATANA_LOG_ASSERT(prop_info.IsClean());

  return new_table;
}

}  // namespace

katana::Result<void>
katana::RDG::UnloadNodeProperty(int i) {
  std::shared_ptr<arrow::Table> new_props = KATANA_CHECKED(UnloadProperty(
      node_properties(), i, &core_->part_header().node_prop_info_list(),
      rdg_dir()));
  core_->set_node_properties(std::move(new_props));
  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDG::UnloadNodeProperty(const std::string& name) {
  auto col_names = node_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), name);
  if (pos != col_names.cend()) {
    return UnloadNodeProperty(std::distance(col_names.cbegin(), pos));
  }
  return KATANA_ERROR(
      katana::ErrorCode::PropertyNotFound, "property {} not found",
      std::quoted(name));
}

katana::Result<katana::Uri>
katana::RDG::GetNodePropertyStorageLocation(const std::string& name) const {
  return GetStorageLocationIfValid(
      name, core_->part_header().node_prop_info_list());
}

katana::Result<void>
katana::RDG::UnloadEdgeProperty(int i) {
  std::shared_ptr<arrow::Table> new_props = KATANA_CHECKED(UnloadProperty(
      edge_properties(), i, &core_->part_header().edge_prop_info_list(),
      rdg_dir()));
  core_->set_edge_properties(std::move(new_props));
  return katana::ResultSuccess();
}

katana::Result<katana::Uri>
katana::RDG::GetEdgePropertyStorageLocation(const std::string& name) const {
  return GetStorageLocationIfValid(
      name, core_->part_header().edge_prop_info_list());
}

katana::Result<void>
katana::RDG::UnloadEdgeProperty(const std::string& name) {
  auto col_names = edge_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), name);
  if (pos != col_names.cend()) {
    return UnloadEdgeProperty(std::distance(col_names.cbegin(), pos));
  }
  return KATANA_ERROR(
      katana::ErrorCode::PropertyNotFound, "property {} not found",
      std::quoted(name));
}

katana::Result<void>
katana::RDG::LoadNodeProperty(const std::string& name, int i) {
  std::shared_ptr<arrow::Table> new_props = KATANA_CHECKED(LoadProperty(
      node_properties(), name, i, &core_->part_header().node_prop_info_list(),
      rdg_dir()));
  core_->set_node_properties(std::move(new_props));
  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDG::LoadEdgeProperty(const std::string& name, int i) {
  std::shared_ptr<arrow::Table> new_props = KATANA_CHECKED(LoadProperty(
      edge_properties(), name, i, &core_->part_header().edge_prop_info_list(),
      rdg_dir()));
  core_->set_edge_properties(std::move(new_props));
  return katana::ResultSuccess();
}

std::vector<std::string>
katana::RDG::ListFullNodeProperties() const {
  std::vector<std::string> result;
  for (const auto& prop : core_->part_header().node_prop_info_list()) {
    result.emplace_back(prop.name());
  }
  return result;
}

std::vector<std::string>
katana::RDG::ListLoadedNodeProperties() const {
  std::vector<std::string> result;
  for (const auto& prop : core_->part_header().node_prop_info_list()) {
    if (!prop.IsAbsent()) {
      result.emplace_back(prop.name());
    }
  }
  return result;
}

std::vector<std::string>
katana::RDG::ListFullEdgeProperties() const {
  std::vector<std::string> result;
  for (const auto& prop : core_->part_header().edge_prop_info_list()) {
    result.emplace_back(prop.name());
  }
  return result;
}

std::vector<std::string>
katana::RDG::ListLoadedEdgeProperties() const {
  std::vector<std::string> result;
  for (const auto& prop : core_->part_header().edge_prop_info_list()) {
    if (!prop.IsAbsent()) {
      result.emplace_back(prop.name());
    }
  }
  return result;
}

const katana::PartitionMetadata&
katana::RDG::part_metadata() const {
  return core_->part_header().metadata();
}

void
katana::RDG::set_part_metadata(const katana::PartitionMetadata& metadata) {
  core_->part_header().set_metadata(metadata);
}

const katana::Uri&
katana::RDG::rdg_dir() const {
  return core_->rdg_dir();
}
void
katana::RDG::set_rdg_dir(const katana::Uri& rdg_dir) {
  return core_->set_rdg_dir(rdg_dir);
}

uint32_t
katana::RDG::partition_id() const {
  return core_->partition_id();
}

const std::shared_ptr<arrow::Table>&
katana::RDG::node_properties() const {
  return core_->node_properties();
}

const std::shared_ptr<arrow::Table>&
katana::RDG::edge_properties() const {
  return core_->edge_properties();
}

void
katana::RDG::DropNodeProperties() {
  core_->drop_node_properties();
}

void
katana::RDG::DropEdgeProperties() {
  core_->drop_edge_properties();
}

katana::Result<void>
katana::RDG::DropAllTopologies() {
  return core_->UnbindAllTopologyFile();
}

std::shared_ptr<arrow::Schema>
katana::RDG::full_node_schema() const {
  return core_->full_node_schema();
}

std::shared_ptr<arrow::Schema>
katana::RDG::full_edge_schema() const {
  return core_->full_edge_schema();
}

const std::vector<std::shared_ptr<arrow::ChunkedArray>>&
katana::RDG::master_nodes() const {
  return core_->master_nodes();
}
const std::vector<std::shared_ptr<arrow::ChunkedArray>>&
katana::RDG::mirror_nodes() const {
  return core_->mirror_nodes();
}
const std::shared_ptr<arrow::ChunkedArray>&
katana::RDG::host_to_owned_global_node_ids() const {
  return core_->host_to_owned_global_node_ids();
}
const std::shared_ptr<arrow::ChunkedArray>&
katana::RDG::host_to_owned_global_edge_ids() const {
  return core_->host_to_owned_global_edge_ids();
}
const std::shared_ptr<arrow::ChunkedArray>&
katana::RDG::local_to_user_id() const {
  return core_->local_to_user_id();
}
const std::shared_ptr<arrow::ChunkedArray>&
katana::RDG::local_to_global_id() const {
  return core_->local_to_global_id();
}

void
katana::RDG::set_master_nodes(
    std::vector<std::shared_ptr<arrow::ChunkedArray>>&& master_nodes) {
  return core_->set_master_nodes(std::move(master_nodes));
}
void
katana::RDG::set_mirror_nodes(
    std::vector<std::shared_ptr<arrow::ChunkedArray>>&& mirror_nodes) {
  return core_->set_mirror_nodes(std::move(mirror_nodes));
}
void
katana::RDG::set_host_to_owned_global_node_ids(
    std::shared_ptr<arrow::ChunkedArray>&& host_to_owned_global_node_ids) {
  return core_->set_host_to_owned_global_node_ids(
      std::move(host_to_owned_global_node_ids));
}
void
katana::RDG::set_host_to_owned_global_edge_ids(
    std::shared_ptr<arrow::ChunkedArray>&& host_to_owned_global_edge_ids) {
  return core_->set_host_to_owned_global_edge_ids(
      std::move(host_to_owned_global_edge_ids));
}
void
katana::RDG::set_local_to_user_id(
    std::shared_ptr<arrow::ChunkedArray>&& local_to_user_id) {
  return core_->set_local_to_user_id(std::move(local_to_user_id));
}
void
katana::RDG::set_local_to_global_id(
    std::shared_ptr<arrow::ChunkedArray>&& local_to_global_id) {
  return core_->set_local_to_global_id(std::move(local_to_global_id));
}

katana::Result<void>
katana::RDG::AddCSRTopologyByFile(
    const katana::Uri& new_top, uint64_t num_nodes, uint64_t num_edges) {
  katana::Uri dir = new_top.DirName();
  if (dir != rdg_dir()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "new topology file must be in this RDG's directory ({})", rdg_dir());
  }
  return core_->RegisterCSRTopologyFile(
      new_top.BaseName(), rdg_dir(), num_nodes, num_edges);
}

katana::Result<katana::RDGTopology*>
katana::RDG::GetTopology(const katana::RDGTopology& shadow) {
  RDGTopology* topology =
      KATANA_CHECKED(core_->topology_manager().GetTopology(shadow));
  KATANA_CHECKED(topology->Bind(rdg_dir()));
  KATANA_CHECKED(topology->Map());
  return topology;
}

const katana::FileView&
katana::RDG::node_entity_type_id_array_file_storage() const {
  return core_->node_entity_type_id_array_file_storage();
}

katana::Result<katana::EntityTypeManager>
katana::RDG::node_entity_type_manager() const {
  return core_->part_header().GetNodeEntityTypeManager();
}

katana::Result<void>
katana::RDG::UnbindNodeEntityTypeIDArrayFileStorage() {
  return core_->node_entity_type_id_array_file_storage().Unbind();
}

katana::Result<void>
katana::RDG::SetNodeEntityTypeIDArrayFile(
    const katana::Uri& new_type_id_array) {
  katana::Uri dir = new_type_id_array.DirName();
  if (dir != rdg_dir()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "new Node Entity Type ID file must be in this RDG's directory ({})",
        rdg_dir());
  }
  return core_->RegisterNodeEntityTypeIDArrayFile(new_type_id_array.BaseName());
}

katana::Result<katana::NUMAArray<katana::EntityTypeID>>
katana::RDG::node_entity_type_id_array() const {
  return KATANA_CHECKED(core_->node_entity_type_id_array());
}

const katana::FileView&
katana::RDG::edge_entity_type_id_array_file_storage() const {
  return core_->edge_entity_type_id_array_file_storage();
}

katana::Result<katana::EntityTypeManager>
katana::RDG::edge_entity_type_manager() const {
  return core_->part_header().GetEdgeEntityTypeManager();
}

katana::Result<void>
katana::RDG::UnbindEdgeEntityTypeIDArrayFileStorage() {
  return core_->edge_entity_type_id_array_file_storage().Unbind();
}

katana::Result<void>
katana::RDG::SetEdgeEntityTypeIDArrayFile(
    const katana::Uri& new_type_id_array) {
  katana::Uri dir = new_type_id_array.DirName();
  if (dir != rdg_dir()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "new Edge Entity Type ID file must be in this RDG's directory ({})",
        rdg_dir());
  }
  return core_->RegisterEdgeEntityTypeIDArrayFile(new_type_id_array.BaseName());
}

katana::Result<katana::NUMAArray<katana::EntityTypeID>>
katana::RDG::edge_entity_type_id_array() const {
  return KATANA_CHECKED(core_->edge_entity_type_id_array());
}

katana::Result<std::optional<katana::RDKLSHIndexPrimitive>>
katana::RDG::LoadRDKLSHIndexPrimitive() {
  if (!KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat)) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "The UnstableRDGStorageFormat feature flag must be set to use this "
        "feature");
  }
  std::optional<std::string> res =
      KATANA_CHECKED(core_->part_header().OptionalDatastructureManifest(
          kOptionalDatastructureRDKLSHIndexPrimitive));
  if (!res) {
    return std::nullopt;
  }

  katana::RDKLSHIndexPrimitive index = KATANA_CHECKED_CONTEXT(
      katana::RDKLSHIndexPrimitive::Load(rdg_dir(), res.value()),
      "Failed to load RDKLSHIndexPrimitive located at {}", res.value());
  return index;
}

katana::Result<void>
katana::RDG::WriteRDKLSHIndexPrimitive(katana::RDKLSHIndexPrimitive& index) {
  if (!KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat)) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "The UnstableRDGStorageFormat feature flag must be set to use this "
        "feature");
  }
  std::string path = KATANA_CHECKED(index.Write(rdg_dir()));
  core_->part_header().AppendOptionalDatastructureManifest(
      kOptionalDatastructureRDKLSHIndexPrimitive, path);

  return katana::ResultSuccess();
}

katana::Result<std::optional<katana::RDKSubstructureIndexPrimitive>>
katana::RDG::LoadRDKSubstructureIndexPrimitive() {
  if (!KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat)) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "The UnstableRDGStorageFormat feature flag must be set to use this "
        "feature");
  }
  std::optional<std::string> res =
      KATANA_CHECKED(core_->part_header().OptionalDatastructureManifest(
          kOptionalDatastructureRDKSubstructureIndexPrimitive));
  if (!res) {
    return std::nullopt;
  }

  katana::RDKSubstructureIndexPrimitive index = KATANA_CHECKED_CONTEXT(
      katana::RDKSubstructureIndexPrimitive::Load(rdg_dir(), res.value()),
      "Failed to load RDKSubstructureIndexPrimitive located at {}",
      res.value());
  return index;
}

katana::Result<void>
katana::RDG::WriteRDKSubstructureIndexPrimitive(
    katana::RDKSubstructureIndexPrimitive& index) {
  if (!KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat)) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "The UnstableRDGStorageFormat feature flag must be set to use this "
        "feature");
  }
  std::string path = KATANA_CHECKED(index.Write(rdg_dir()));
  core_->part_header().AppendOptionalDatastructureManifest(
      kOptionalDatastructureRDKSubstructureIndexPrimitive, path);

  return katana::ResultSuccess();
}

katana::RDG::RDG(std::unique_ptr<RDGCore>&& core) : core_(std::move(core)) {}

katana::RDG::RDG() : core_(std::make_unique<RDGCore>()) {}

katana::RDG::~RDG() = default;
katana::RDG::RDG(katana::RDG&& other) noexcept = default;
katana::RDG& katana::RDG::operator=(katana::RDG&& other) noexcept = default;
