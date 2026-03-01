//! DevContext - TercenContext implementation for development/testing mode
//!
//! Initialized from workflow_id + step_id, fetches data from workflow structure.
//! This mirrors Python's OperatorContextDev.

use super::base::{ContextBase, ContextBaseBuilder};
use super::TercenContext;
use crate::client::proto::{CubeQuery, OperatorSettings};
use crate::colors::{ChartKind, ColorInfo};
use crate::TercenClient;
use std::ops::Deref;
use std::sync::Arc;

/// Development context initialized from workflow_id + step_id
///
/// This is used for local testing when we don't have a task_id.
/// Wraps ContextBase using the newtype pattern.
pub struct DevContext(ContextBase);

impl Deref for DevContext {
    type Target = ContextBase;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DevContext {
    /// Create a new DevContext from workflow_id and step_id
    ///
    /// This fetches the workflow, finds the step, and extracts the CubeQuery
    /// either from the step's model.task_id or by calling getCubeQuery.
    pub async fn from_workflow_step(
        client: Arc<TercenClient>,
        workflow_id: &str,
        step_id: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use crate::client::proto::{e_step, e_task, e_workflow, GetRequest};

        println!("[DevContext] Fetching workflow {}...", workflow_id);

        // Fetch workflow
        let mut workflow_service = client.workflow_service()?;
        let request = tonic::Request::new(GetRequest {
            id: workflow_id.to_string(),
            ..Default::default()
        });
        let response = workflow_service.get(request).await?;
        let e_workflow = response.into_inner();

        let workflow = match e_workflow.object {
            Some(e_workflow::Object::Workflow(wf)) => wf,
            _ => return Err("No workflow object".into()),
        };

        println!("[DevContext] Workflow name: {}", workflow.name);

        // Find the DataStep
        let data_step = workflow
            .steps
            .iter()
            .find_map(|e_step| {
                if let Some(e_step::Object::Datastep(ds)) = &e_step.object {
                    if ds.id == step_id {
                        return Some(ds.clone());
                    }
                }
                None
            })
            .ok_or_else(|| format!("DataStep {} not found in workflow", step_id))?;

        println!("[DevContext] Step name: {}", data_step.name);

        // Get task_id from model (if exists)
        let task_id = data_step
            .model
            .as_ref()
            .map(|m| m.task_id.clone())
            .unwrap_or_default();

        println!("[DevContext] Model task_id: '{}'", task_id);

        // Get CubeQuery and schema_ids
        let (cube_query, schema_ids, project_id) = if task_id.is_empty() {
            // No task_id - call getCubeQuery
            println!("[DevContext] Calling getCubeQuery...");
            let mut workflow_service = client.workflow_service()?;
            let request = tonic::Request::new(crate::client::proto::ReqGetCubeQuery {
                workflow_id: workflow_id.to_string(),
                step_id: step_id.to_string(),
            });
            let response = workflow_service.get_cube_query(request).await?;
            let resp = response.into_inner();
            let query = resp.result.ok_or("getCubeQuery returned no result")?;

            // getCubeQuery doesn't return schema_ids, so we can't get Y-axis/color tables this way
            // We'll have to leave schema_ids empty
            (query, Vec::new(), String::new())
        } else {
            // Retrieve task to get CubeQuery and schema_ids
            println!("[DevContext] Retrieving task {}...", task_id);
            let mut task_service = client.task_service()?;
            let request = tonic::Request::new(GetRequest {
                id: task_id.clone(),
                ..Default::default()
            });
            let response = task_service.get(request).await?;
            let task = response.into_inner();

            match task.object.as_ref() {
                Some(e_task::Object::Cubequerytask(cqt)) => {
                    let query = cqt.query.as_ref().ok_or("CubeQueryTask has no query")?;
                    (
                        query.clone(),
                        cqt.schema_ids.clone(),
                        cqt.project_id.clone(),
                    )
                }
                Some(e_task::Object::Computationtask(ct)) => {
                    let query = ct.query.as_ref().ok_or("ComputationTask has no query")?;
                    (query.clone(), ct.schema_ids.clone(), ct.project_id.clone())
                }
                Some(e_task::Object::Runcomputationtask(rct)) => {
                    let query = rct
                        .query
                        .as_ref()
                        .ok_or("RunComputationTask has no query")?;
                    (
                        query.clone(),
                        rct.schema_ids.clone(),
                        rct.project_id.clone(),
                    )
                }
                _ => return Err("Task is not a query task".into()),
            }
        };

        println!("[DevContext] CubeQuery retrieved");
        println!("[DevContext]   qt_hash: {}", cube_query.qt_hash);
        println!("[DevContext]   column_hash: {}", cube_query.column_hash);
        println!("[DevContext]   row_hash: {}", cube_query.row_hash);
        println!(
            "[DevContext]   axis_queries count: {}",
            cube_query.axis_queries.len()
        );
        for (i, aq) in cube_query.axis_queries.iter().enumerate() {
            println!(
                "[DevContext]   axis_queries[{}]: chart_type='{}', point_size={}, colors={:?}",
                i,
                aq.chart_type,
                aq.point_size,
                aq.colors.iter().map(|f| &f.name).collect::<Vec<_>>()
            );
        }

        // Extract operator settings and namespace from cube_query
        let operator_settings = cube_query.operator_settings.clone();
        let namespace = operator_settings
            .as_ref()
            .map(|os| os.namespace.clone())
            .unwrap_or_default();

        // Find Y-axis table
        let y_axis_table_id = if !schema_ids.is_empty() {
            super::helpers::find_y_axis_table(&client, &schema_ids, &cube_query, "DevContext")
                .await?
        } else {
            None
        };

        // Find X-axis table
        let x_axis_table_id = if !schema_ids.is_empty() {
            super::helpers::find_x_axis_table(&client, &schema_ids, &cube_query, "DevContext")
                .await?
        } else {
            None
        };

        // Extract per-layer color information
        let per_layer_colors = super::helpers::extract_per_layer_color_info_from_workflow(
            &client,
            &schema_ids,
            &workflow,
            step_id,
            "DevContext",
        )
        .await?;

        // Also extract legacy color_infos for backwards compatibility
        let color_infos = super::helpers::extract_color_info_from_workflow(
            &client,
            &schema_ids,
            &workflow,
            step_id,
            "DevContext",
        )
        .await?;

        // Extract page factors
        let page_factors = crate::extract_page_factors(operator_settings.as_ref());

        // Extract point size from workflow step
        let point_size = match crate::extract_point_size_from_step(&workflow, step_id) {
            Ok(ps) => ps,
            Err(e) => {
                eprintln!("[DevContext] Failed to extract point_size: {}", e);
                None
            }
        };

        // Extract chart kind from workflow step
        let chart_kind = match crate::extract_chart_kind_from_step(&workflow, step_id) {
            Ok(ck) => {
                println!("[DevContext] Chart kind: {:?}", ck);
                ck
            }
            Err(e) => {
                eprintln!("[DevContext] Failed to extract chart_kind: {}", e);
                ChartKind::Point
            }
        };

        // Extract crosstab dimensions from step model
        let crosstab_dimensions = super::helpers::extract_crosstab_dimensions(&workflow, step_id);
        if let Some((w, h)) = crosstab_dimensions {
            println!("[DevContext] Crosstab dimensions: {}×{} pixels", w, h);
        }

        // Extract axis transforms from Crosstab model (not CubeQuery)
        // Transforms are in step.model.axis.xyAxis[0].preprocessors
        let (y_transform, x_transform) =
            super::helpers::extract_transforms_from_step(&workflow, step_id, &cube_query);

        // Extract layer palette name from GlTask (preferred) or fallback to crosstab palette
        let layer_palette_name =
            match super::helpers::extract_layer_palette_from_gltask(&client, &workflow, step_id)
                .await
            {
                Ok(Some(name)) => {
                    println!("[DevContext] Layer palette (from GlTask): {}", name);
                    Some(name)
                }
                Ok(None) | Err(_) => {
                    // Fallback to crosstab palette extraction
                    let name = crate::extract_crosstab_palette_name(&workflow, step_id);
                    if let Some(ref n) = name {
                        println!("[DevContext] Layer palette (from crosstab): {}", n);
                    }
                    name
                }
            };

        // Extract Y-axis factor names per layer (for legend entries)
        let layer_y_factor_names = super::helpers::extract_layer_y_factor_names(&workflow, step_id);
        if !layer_y_factor_names.is_empty() {
            println!(
                "[DevContext] Layer Y-factor names: {:?}",
                layer_y_factor_names
            );
        }

        // Build ContextBase using the builder
        let base = ContextBaseBuilder::new()
            .client(client)
            .cube_query(cube_query)
            .schema_ids(schema_ids)
            .workflow_id(workflow_id.to_string())
            .step_id(step_id.to_string())
            .project_id(project_id)
            .namespace(namespace)
            .operator_settings(operator_settings)
            .color_infos(color_infos)
            .per_layer_colors(Some(per_layer_colors))
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
}

impl TercenContext for DevContext {
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
