use std::{collections::HashMap, ops::Range, path::PathBuf};

use clap::Parser;
use dslab_mr::trace::{Trace, TraceEvent};
use plotters::{
    backend::BitMapBackend,
    chart::{ChartBuilder, SeriesLabelPosition},
    drawing::IntoDrawingArea,
    element::Rectangle,
    series::LineSeries,
    style::{Color, RGBAColor, ShapeStyle, BLACK, BLUE, GREEN, RED, WHITE},
};

/// Draws some graphs from simulation trace.
#[derive(Parser, Debug)]
struct Args {
    /// Path to trace file.
    #[arg(short, long)]
    trace: PathBuf,

    /// Path to output folder with graphs.
    #[arg(short, long)]
    output: PathBuf,

    /// Start of a time segment for a graph.
    #[arg(long, default_value = None)]
    from: Option<f32>,

    /// End of a time segment for a graph.
    #[arg(long, default_value = None)]
    to: Option<f32>,

    /// Width of a graph in pixels.
    #[arg(long, default_value_t = 1920)]
    width: u32,

    /// Height of a graph in pixels.
    #[arg(long, default_value_t = 1080)]
    height: u32,

    /// Enable all graphs.
    #[arg(long, default_value_t = false)]
    all: bool,

    /// Add cpu utilization to the graph.
    #[arg(long, default_value_t = false)]
    cpu_utilization: bool,

    /// Add memory utilization to the graph.
    #[arg(long, default_value_t = false)]
    memory_utilization: bool,

    /// Add graph with the number of ready tasks.
    #[arg(long, default_value_t = false)]
    ready_tasks: bool,

    /// Add graph with the number of queued tasks.
    #[arg(long, default_value_t = false)]
    queued_tasks: bool,

    /// Add graph with the number of assigned tasks.
    #[arg(long, default_value_t = false)]
    assigned_tasks: bool,

    /// Add graph with the number of running tasks.
    #[arg(long, default_value_t = false)]
    running_tasks: bool,

    /// Add graph with the number of running DAGs.
    #[arg(long, default_value_t = false)]
    running_dags: bool,

    /// Use 0-100 scale for utilization instead of 0-1.
    #[arg(long, default_value_t = false)]
    percent: bool,

    /// Draw full y axis instead of up to maximum usage.
    #[arg(long, default_value_t = false)]
    fully: bool,

    /// Stroke width.
    #[arg(long, default_value_t = 2)]
    stroke_width: u32,

    /// Font size.
    #[arg(long, default_value_t = 25)]
    font_size: u32,
}

#[derive(Copy, Clone)]
enum MetricChange {
    Add {
        event_path: (usize, usize, usize),
        value: f32,
    },
    Remove {
        event_path: (usize, usize, usize),
    },
    None,
}

fn collect_usage_graph<F>(trace: &Trace, args: &Args, metric: F) -> Vec<(f32, f32)>
where
    F: Fn(&TraceEvent) -> MetricChange,
{
    let mut points = Vec::new();
    let mut cur_usage = 0.0;
    let from = args.from.unwrap_or(trace.events[0].time() as f32);
    let to = args.to.unwrap_or(trace.events.last().unwrap().time() as f32);
    let time_range = to - from;

    let mut added: HashMap<(usize, usize, usize), f32> = HashMap::new();
    for event in trace.events.iter() {
        let time = event.time() as f32;
        if time < from {
            continue;
        }
        if time >= from && points.is_empty() {
            points.push((time, cur_usage));
        }
        if time > to {
            points.push((time, cur_usage));
            break;
        }

        cur_usage += match metric(event) {
            MetricChange::Add { event_path, value } => {
                added.insert(event_path, value);
                value
            }
            MetricChange::Remove { event_path } => -added.remove(&event_path).unwrap(),
            MetricChange::None {} => 0.,
        };

        if points.is_empty() {
            points.push((time, 0.0));
        } else {
            let lasty = points.last().unwrap().1;
            points.push((time, lasty));
        }
        while points.len() >= 2 && points[points.len() - 2].0 + time_range / 1000.0 >= time {
            points.pop();
        }
        points.push((time, cur_usage));
    }
    points
}

struct Line {
    name: String,
    color: RGBAColor,
    data: Vec<(f32, f32)>,
}

fn draw_utilization_graphs(args: &Args, trace: &Trace) {
    let path = args.output.join("utilization.png");
    let root = BitMapBackend::new(&path, (args.width, args.height)).into_drawing_area();
    let _ = root.fill(&WHITE);

    let total_cores: u32 = trace.hosts.iter().map(|host| host.available_cores).sum();
    let total_memory: u64 = trace.hosts.iter().map(|host| host.available_memory).sum();

    let mut lines: Vec<Line> = Vec::new();

    if args.cpu_utilization || args.all {
        lines.push(Line {
            name: "CPU utilization".to_string(),
            color: BLUE.into(),
            data: collect_usage_graph(trace, args, |event| match event {
                TraceEvent::TaskStarted { cores, .. } => MetricChange::Add {
                    event_path: event.task_path().unwrap(),
                    value: *cores as f32 / total_cores as f32,
                },
                TraceEvent::TaskCompleted { .. } => MetricChange::Remove {
                    event_path: event.task_path().unwrap(),
                },
                _ => MetricChange::None,
            }),
        });
    }
    if args.memory_utilization || args.all {
        lines.push(Line {
            name: "Memory utilization".to_string(),
            color: RED.into(),
            data: collect_usage_graph(trace, args, |event| match event {
                TraceEvent::TaskStarted { memory, .. } => MetricChange::Add {
                    event_path: event.task_path().unwrap(),
                    value: *memory as f32 / total_memory as f32,
                },
                TraceEvent::TaskCompleted { .. } => MetricChange::Remove {
                    event_path: event.task_path().unwrap(),
                },
                _ => MetricChange::None,
            }),
        });
    }
    let mut maxy: f32 = if args.fully { 1.0 } else { 0.0 };
    for line in lines.iter_mut() {
        for &(_x, y) in line.data.iter() {
            maxy = maxy.max(y);
        }
        if args.percent {
            for (_x, y) in line.data.iter_mut() {
                *y *= 100.0;
            }
        }
    }
    if args.percent {
        maxy *= 100.0;
    }

    let root = root.margin(20, 20, 20, 20);
    let mut chart = ChartBuilder::on(&root)
        .x_label_area_size(20)
        .y_label_area_size(40)
        .build_cartesian_2d(
            {
                let mut range = lines
                    .iter()
                    .flat_map(|line| line.data.iter())
                    .map(|event| event.0)
                    .fold(
                        Range {
                            start: f32::MAX,
                            end: 0.0,
                        },
                        |range, time| Range {
                            start: range.start.min(time),
                            end: range.end.max(time),
                        },
                    );
                if let Some(from) = args.from {
                    range.start = from;
                }
                if let Some(to) = args.to {
                    range.end = to;
                }
                range
            },
            0.0..maxy,
        )
        .unwrap();

    chart
        .configure_mesh()
        .x_label_style(("sans-serif", args.font_size, &BLACK))
        .y_label_style(("sans-serif", args.font_size, &BLACK))
        .draw()
        .unwrap();

    for line in lines.into_iter() {
        chart
            .draw_series(LineSeries::new(
                line.data,
                ShapeStyle {
                    color: line.color,
                    filled: false,
                    stroke_width: args.stroke_width,
                },
            ))
            .unwrap()
            .label(line.name)
            .legend(move |(x, y)| Rectangle::new([(x - 10, y + 2), (x + 10, y - 2)], line.color.filled()));
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .label_font(("sans-serif", args.font_size, &BLACK))
        .margin(20)
        .legend_area_size(25)
        .border_style(BLUE)
        .background_style(BLUE.mix(0.1))
        .draw()
        .unwrap();
    root.present().unwrap();
}

fn draw_running_tasks(args: &Args, trace: &Trace) {
    let path = args.output.join("tasks.png");
    let root = BitMapBackend::new(&path, (args.width, args.height)).into_drawing_area();
    let _ = root.fill(&WHITE);

    let mut lines: Vec<Line> = Vec::new();

    if args.ready_tasks || args.all {
        lines.push(Line {
            name: "Ready tasks".to_string(),
            color: BLUE.into(),
            data: collect_usage_graph(trace, args, |event| match event {
                TraceEvent::TaskReady { .. } => MetricChange::Add {
                    event_path: event.task_path().unwrap(),
                    value: 1f32,
                },
                TraceEvent::TaskInQueue { .. } => MetricChange::Remove {
                    event_path: event.task_path().unwrap(),
                },
                _ => MetricChange::None,
            }),
        });
    }
    if args.queued_tasks || args.all {
        lines.push(Line {
            name: "Queued tasks".to_string(),
            color: RED.into(),
            data: collect_usage_graph(trace, args, |event| match event {
                TraceEvent::TaskInQueue { .. } => MetricChange::Add {
                    event_path: event.task_path().unwrap(),
                    value: 1f32,
                },
                TraceEvent::TaskAssigned { .. } => MetricChange::Remove {
                    event_path: event.task_path().unwrap(),
                },
                _ => MetricChange::None,
            }),
        });
    }
    if args.assigned_tasks || args.all {
        lines.push(Line {
            name: "Assigned tasks".to_string(),
            color: GREEN.into(),
            data: collect_usage_graph(trace, args, |event| match event {
                TraceEvent::TaskAssigned { .. } => MetricChange::Add {
                    event_path: event.task_path().unwrap(),
                    value: 1f32,
                },
                TraceEvent::TaskStarted { .. } => MetricChange::Remove {
                    event_path: event.task_path().unwrap(),
                },
                _ => MetricChange::None,
            }),
        });
    }
    if args.running_tasks || args.all {
        lines.push(Line {
            name: "Running tasks".to_string(),
            color: BLACK.into(),
            data: collect_usage_graph(trace, args, |event| match event {
                TraceEvent::TaskStarted { .. } => MetricChange::Add {
                    event_path: event.task_path().unwrap(),
                    value: 1f32,
                },
                TraceEvent::TaskCompleted { .. } => MetricChange::Remove {
                    event_path: event.task_path().unwrap(),
                },
                _ => MetricChange::None,
            }),
        });
    }

    let maxy = lines
        .iter()
        .flat_map(|line| line.data.iter().map(|(_x, y)| *y))
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();

    let root = root.margin(20, 20, 20, 20);
    let mut chart = ChartBuilder::on(&root)
        .x_label_area_size(20)
        .y_label_area_size(40)
        .build_cartesian_2d(
            {
                let mut range = lines
                    .iter()
                    .flat_map(|line| line.data.iter())
                    .map(|event| event.0)
                    .fold(
                        Range {
                            start: f32::MAX,
                            end: 0.0,
                        },
                        |range, time| Range {
                            start: range.start.min(time),
                            end: range.end.max(time),
                        },
                    );
                if let Some(from) = args.from {
                    range.start = from;
                }
                if let Some(to) = args.to {
                    range.end = to;
                }
                range
            },
            0.0..maxy,
        )
        .unwrap();

    chart
        .configure_mesh()
        .x_label_style(("sans-serif", args.font_size, &BLACK))
        .y_label_style(("sans-serif", args.font_size, &BLACK))
        .y_label_formatter(&|x: &f32| (x.round() as usize).to_string())
        .draw()
        .unwrap();

    for line in lines.into_iter() {
        chart
            .draw_series(LineSeries::new(
                line.data,
                ShapeStyle {
                    color: line.color,
                    filled: false,
                    stroke_width: args.stroke_width,
                },
            ))
            .unwrap()
            .label(line.name)
            .legend(move |(x, y)| Rectangle::new([(x - 10, y + 2), (x + 10, y - 2)], line.color.filled()));
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .label_font(("sans-serif", args.font_size, &BLACK))
        .margin(20)
        .legend_area_size(25)
        .border_style(BLUE)
        .background_style(BLUE.mix(0.1))
        .draw()
        .unwrap();
    root.present().unwrap();
}

fn draw_running_dags(args: &Args, trace: &Trace) {
    let path = args.output.join("dags.png");
    let root = BitMapBackend::new(&path, (args.width, args.height)).into_drawing_area();
    let _ = root.fill(&WHITE);

    let lines: Vec<Line> = vec![Line {
        name: "Running DAGs".to_string(),
        color: BLUE.into(),
        data: collect_usage_graph(trace, args, |event| match event {
            TraceEvent::DagStarted { dag_id, .. } => MetricChange::Add {
                event_path: (*dag_id, 0, 0),
                value: 1f32,
            },
            TraceEvent::DagCompleted { dag_id, .. } => MetricChange::Remove {
                event_path: (*dag_id, 0, 0),
            },
            _ => MetricChange::None,
        }),
    }];

    let maxy = lines
        .iter()
        .flat_map(|line| line.data.iter().map(|(_x, y)| *y))
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();

    let root = root.margin(20, 20, 20, 20);
    let mut chart = ChartBuilder::on(&root)
        .x_label_area_size(20)
        .y_label_area_size(40)
        .build_cartesian_2d(
            {
                let mut range = lines
                    .iter()
                    .flat_map(|line| line.data.iter())
                    .map(|event| event.0)
                    .fold(
                        Range {
                            start: f32::MAX,
                            end: 0.0,
                        },
                        |range, time| Range {
                            start: range.start.min(time),
                            end: range.end.max(time),
                        },
                    );
                if let Some(from) = args.from {
                    range.start = from;
                }
                if let Some(to) = args.to {
                    range.end = to;
                }
                range
            },
            0.0..maxy,
        )
        .unwrap();

    chart
        .configure_mesh()
        .x_label_style(("sans-serif", args.font_size, &BLACK))
        .y_label_style(("sans-serif", args.font_size, &BLACK))
        .y_label_formatter(&|x: &f32| (x.round() as usize).to_string())
        .draw()
        .unwrap();

    for line in lines.into_iter() {
        chart
            .draw_series(LineSeries::new(
                line.data,
                ShapeStyle {
                    color: line.color,
                    filled: false,
                    stroke_width: args.stroke_width,
                },
            ))
            .unwrap()
            .label(line.name)
            .legend(move |(x, y)| Rectangle::new([(x - 10, y + 2), (x + 10, y - 2)], line.color.filled()));
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .label_font(("sans-serif", args.font_size, &BLACK))
        .margin(20)
        .legend_area_size(25)
        .border_style(BLUE)
        .background_style(BLUE.mix(0.1))
        .draw()
        .unwrap();
    root.present().unwrap();
}

fn main() {
    let args = Args::parse();
    let trace: Trace = serde_json::from_str(&std::fs::read_to_string(&args.trace).expect("Can't read trace file"))
        .expect("Can't parse trace file as json");
    std::fs::create_dir_all(&args.output).expect("Can't create output folder");

    if args.cpu_utilization || args.memory_utilization || args.all {
        draw_utilization_graphs(&args, &trace);
    }
    if args.ready_tasks || args.queued_tasks || args.assigned_tasks || args.running_tasks || args.all {
        draw_running_tasks(&args, &trace);
    }
    if args.running_dags || args.all {
        draw_running_dags(&args, &trace);
    }
}
