//! ProductionContext - TercenContext implementation for production mode
//!
//! Initialized from a task_id, extracts all necessary data from the task object.

use super::base::{ContextBase, ContextBaseBuilder};
use super::TercenContext;
use crate::client::proto::{CubeQuery, OperatorSettings};
use crate::colors::{ChartKind, ColorInfo};
use crate::TercenClient;
use std::ops::Deref;
use std::sync::Arc;

/// Production context initialized from task_id
///
/// This is used when the operator is run by Tercen with --taskId argument.
/// Wraps ContextBase using the newtype pattern.
pub struct ProductionContext(ContextBase);

impl Deref for ProductionContext {
    type Target = ContextBase;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ProductionContext {
    /// Create a new ProductionContext from a task_id
    ///
    /// This fetches the task, extracts the CubeQuery, and retrieves schema_ids.
    /// Schema_ids are first checked on the task itself; if empty, they are
    /// fetched from the parent CubeQueryTask via parentTaskId.
    pub async fn from_task_id(
        client: Arc<TercenClient>,
        task_id: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use crate::client::proto::{e_task, GetRequest};

        println!("[ProductionContext] Fetching task {}...", task_id);

        // Fetch the operator task
        let mut task_service = client.task_service()?;
        let request = tonic::Request::new(GetRequest {
            id: task_id.to_string(),
            ..Default::default()
        });
        let response = task_service.get(request).await?;
        let task = response.into_inner();

        // Extract CubeQuery, schema_ids, parent_task_id, and metadata from task
        let (cube_query, project_id, operator_settings, task_environment, mut schema_ids, parent_task_id) =
            match task.object.as_ref() {
                Some(e_task::Object::Computationtask(ct)) => (
                    ct.query
                        .as_ref()
                        .ok_or("ComputationTask has no query")?
                        .clone(),
                    ct.project_id.clone(),
                    ct.query.as_ref().and_then(|q| q.operator_settings.clone()),
                    &ct.environment,
                    ct.schema_ids.clone(),
                    ct.parent_task_id.clone(),
                ),
                Some(e_task::Object::Runcomputationtask(rct)) => (
                    rct.query
                        .as_ref()
                        .ok_or("RunComputationTask has no query")?
                        .clone(),
                    rct.project_id.clone(),
                    rct.query.as_ref().and_then(|q| q.operator_settings.clone()),
                    &rct.environment,
                    rct.schema_ids.clone(),
                    rct.parent_task_id.clone(),
                ),
                Some(e_task::Object::Cubequerytask(cqt)) => (
                    cqt.query
                        .as_ref()
                        .ok_or("CubeQueryTask has no query")?
                        .clone(),
                    cqt.project_id.clone(),
                    cqt.query.as_ref().and_then(|q| q.operator_settings.clone()),
                    &cqt.environment,
                    cqt.schema_ids.clone(),
                    String::new(), // CubeQueryTask has no parent
                ),
                _ => return Err("Unsupported task type".into()),
            };

        // Extract namespace from operator settings
        let namespace = operator_settings
            .as_ref()
            .map(|os| os.namespace.clone())
            .unwrap_or_default();

        // Get workflow_id and step_id from task environment (optional for computation operators)
        let workflow_id = task_environment
            .iter()
            .find(|p| p.key == "workflow.id")
            .map(|p| p.value.clone())
            .or_else(|| std::env::var("WORKFLOW_ID").ok())
            .unwrap_or_default();

        let step_id = task_environment
            .iter()
            .find(|p| p.key == "step.id")
            .map(|p| p.value.clone())
            .or_else(|| std::env::var("STEP_ID").ok())
            .unwrap_or_default();

        if !workflow_id.is_empty() {
            println!(
                "[ProductionContext] workflow_id={}, step_id={}",
                workflow_id, step_id
            );
        } else {
            println!("[ProductionContext] No workflow.id in task environment (computation operator mode)");
        }

        // Get schema_ids: try task first, fall back to parent CubeQueryTask via parentTaskId
        if schema_ids.is_empty() && !parent_task_id.is_empty() {
            println!(
                "[ProductionContext] schema_ids empty on task, fetching from parent CubeQueryTask {}",
                parent_task_id
            );
            schema_ids =
                Self::fetch_schema_ids_from_parent_task(&client, &parent_task_id).await?;
        }

        if schema_ids.is_empty() {
            println!("[ProductionContext] WARNING: schema_ids is empty");
        } else {
            println!(
                "[ProductionContext] Found {} schema_ids: {:?}",
                schema_ids.len(),
                schema_ids
            );
        }

        // Find Y-axis table
        let y_axis_table_id = super::helpers::find_y_axis_table(
            &client,
            &schema_ids,
            &cube_query,
            "ProductionContext",
        )
        .await?;

        // Find X-axis table
        let x_axis_table_id = super::helpers::find_x_axis_table(
            &client,
            &schema_ids,
            &cube_query,
            "ProductionContext",
        )
        .await?;

        // Extract page factors from operator settings
        let page_factors = crate::extract_page_factors(operator_settings.as_ref());

        // Extract axis transforms from CubeAxisQuery
        let (y_transform, x_transform) =
            super::helpers::extract_transforms_from_cube_query(&cube_query);

        // Workflow-dependent settings (colors, chart kind, point size, etc.)
        // Only available when workflow.id and step.id are in the task environment.
        // Computation operators (mean, sum, etc.) don't need these.
        let mut per_layer_colors = None;
        let mut color_infos = Vec::new();
        let mut point_size = None;
        let mut chart_kind = ChartKind::Point;
        let mut crosstab_dimensions = None;
        let mut layer_palette_name = None;
        let mut layer_y_factor_names = Vec::new();

        if !workflow_id.is_empty() && !step_id.is_empty() {
            let workflow = super::helpers::fetch_workflow(&client, &workflow_id).await?;

            per_layer_colors = Some(
                super::helpers::extract_per_layer_color_info_from_workflow(
                    &client,
                    &schema_ids,
                    &workflow,
                    &step_id,
                    "ProductionContext",
                )
                .await?,
            );

            color_infos = super::helpers::extract_color_info_from_workflow(
                &client,
                &schema_ids,
                &workflow,
                &step_id,
                "ProductionContext",
            )
            .await?;

            point_size = match crate::extract_point_size_from_step(&workflow, &step_id) {
                Ok(ps) => ps,
                Err(e) => {
                    eprintln!("[ProductionContext] Failed to extract point_size: {}", e);
                    None
                }
            };

            chart_kind = match crate::extract_chart_kind_from_step(&workflow, &step_id) {
                Ok(ck) => {
                    println!("[ProductionContext] Chart kind: {:?}", ck);
                    ck
                }
                Err(e) => {
                    eprintln!("[ProductionContext] Failed to extract chart_kind: {}", e);
                    ChartKind::Point
                }
            };

            crosstab_dimensions =
                super::helpers::extract_crosstab_dimensions(&workflow, &step_id);
            if let Some((w, h)) = crosstab_dimensions {
                println!(
                    "[ProductionContext] Crosstab dimensions: {}×{} pixels",
                    w, h
                );
            }

            layer_palette_name = match super::helpers::extract_layer_palette_from_gltask(
                &client, &workflow, &step_id,
            )
            .await
            {
                Ok(Some(name)) => {
                    println!("[ProductionContext] Layer palette (from GlTask): {}", name);
                    Some(name)
                }
                Ok(None) | Err(_) => {
                    let name = crate::extract_crosstab_palette_name(&workflow, &step_id);
                    if let Some(ref n) = name {
                        println!("[ProductionContext] Layer palette (from crosstab): {}", n);
                    }
                    name
                }
            };

            layer_y_factor_names =
                super::helpers::extract_layer_y_factor_names(&workflow, &step_id);
            if !layer_y_factor_names.is_empty() {
                println!(
                    "[ProductionContext] Layer Y-factor names: {:?}",
                    layer_y_factor_names
                );
            }
        }

        // Build ContextBase using the builder
        let base = ContextBaseBuilder::new()
            .client(client)
            .cube_query(cube_query)
            .schema_ids(schema_ids)
            .workflow_id(workflow_id)
            .step_id(step_id)
            .project_id(project_id)
            .namespace(namespace)
            .operator_settings(operator_settings)
            .color_infos(color_infos)
            .per_layer_colors(per_layer_colors)
            .page_factors(page_factors)
            .y_axis_table_id(y_axis_table_id)
            .x_axis_table_id(x_axis_table_id)
            .point_size(point_size)
            .chart_kind(chart_kind)
            .crosstab_dimensions(crosstab_dimensions)
            .y_transform(y_transform)
            .x_transform(x_transform)
            .layer_palette_name(layer_palette_name)
            .layer_y_factor_names(layer_y_factor_names)
            .build()?;

        Ok(Self(base))
    }

    /// Fetch schema_ids from the parent CubeQueryTask via parentTaskId.
    ///
    /// The worker populates schema_ids on the CubeQueryTask (not the RunComputationTask).
    /// This method fetches the parent task to get them.
    async fn fetch_schema_ids_from_parent_task(
        client: &TercenClient,
        parent_task_id: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        use crate::client::proto::{e_task, GetRequest};

        let mut task_service = client.task_service()?;
        let request = tonic::Request::new(GetRequest {
            id: parent_task_id.to_string(),
            ..Default::default()
        });
        let response = task_service.get(request).await?;
        let task = response.into_inner();

        let schema_ids = match task.object.as_ref() {
            Some(e_task::Object::Cubequerytask(cqt)) => cqt.schema_ids.clone(),
            Some(e_task::Object::Computationtask(ct)) => ct.schema_ids.clone(),
            Some(e_task::Object::Runcomputationtask(rct)) => rct.schema_ids.clone(),
            _ => {
                return Err(format!(
                    "Parent task {} has unexpected type",
                    parent_task_id
                )
                .into())
            }
        };

        if schema_ids.is_empty() {
            return Err(format!(
                "Parent CubeQueryTask {} has empty schema_ids",
                parent_task_id
            )
            .into());
        }

        println!(
            "[ProductionContext] Found {} schema_ids from parent task {}",
            schema_ids.len(),
            parent_task_id
        );

        Ok(schema_ids)
    }
}

impl TercenContext for ProductionContext {
    fn cube_query(&self) -> &CubeQuery {
        self.0.cube_query()
    }

    fn schema_ids(&self) -> &[String] {
        self.0.schema_ids()
    }

    fn workflow_id(&self) -> &str {
        self.0.workflow_id()
    }

    fn step_id(&self) -> &str {
        self.0.step_id()
    }

    fn project_id(&self) -> &str {
        self.0.project_id()
    }

    fn namespace(&self) -> &str {
        self.0.namespace()
    }

    fn operator_settings(&self) -> Option<&OperatorSettings> {
        self.0.operator_settings()
    }

    fn color_infos(&self) -> &[ColorInfo] {
        self.0.color_infos()
    }

    fn page_factors(&self) -> &[String] {
        self.0.page_factors()
    }

    fn y_axis_table_id(&self) -> Option<&str> {
        self.0.y_axis_table_id()
    }

    fn x_axis_table_id(&self) -> Option<&str> {
        self.0.x_axis_table_id()
    }

    fn point_size(&self) -> Option<i32> {
        self.0.point_size()
    }

    fn chart_kind(&self) -> ChartKind {
        self.0.chart_kind()
    }

    fn crosstab_dimensions(&self) -> Option<(i32, i32)> {
        self.0.crosstab_dimensions()
    }

    fn y_transform(&self) -> Option<&str> {
        self.0.y_transform()
    }

    fn x_transform(&self) -> Option<&str> {
        self.0.x_transform()
    }

    fn layer_palette_name(&self) -> Option<&str> {
        self.0.layer_palette_name()
    }

    fn per_layer_colors(&self) -> Option<&crate::PerLayerColorConfig> {
        self.0.per_layer_colors()
    }

    fn layer_y_factor_names(&self) -> &[String] {
        self.0.layer_y_factor_names()
    }

    fn client(&self) -> &Arc<TercenClient> {
        self.0.client()
    }
}
