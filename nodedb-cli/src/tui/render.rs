//! TUI rendering with ratatui.
//!
//! Clean layout like Claude CLI: status bar, results text, separator, prompt.
//! No box-drawing borders anywhere — clean copy/paste.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

use super::app::App;

pub fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();

    let input_height = (input_line_count(app) + 1) as u16;
    let layout = Layout::vertical([
        Constraint::Length(1),            // status bar
        Constraint::Length(1),            // top separator
        Constraint::Min(1),               // results
        Constraint::Length(1),            // bottom separator
        Constraint::Length(input_height), // input
    ])
    .split(area);

    render_status_bar(frame, layout[0], app);
    render_separator(frame, layout[1]);
    render_results(frame, layout[2], app);
    render_separator(frame, layout[3]);
    render_input(frame, layout[4], app);
}

fn render_status_bar(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let style = Style::default().bg(Color::DarkGray).fg(Color::White);

    let fmt = match app.output_format {
        crate::args::OutputFormat::Table => "table",
        crate::args::OutputFormat::Json => "json",
        crate::args::OutputFormat::Csv => "csv",
    };

    let timing = if let Some(d) = app.last_query_time {
        let ms = d.as_secs_f64() * 1000.0;
        if ms >= 60_000.0 {
            format!(" {}m{}s", d.as_secs() / 60, d.as_secs() % 60)
        } else if ms >= 1000.0 {
            format!(" {:.1}s", d.as_secs_f64())
        } else {
            format!(" {:.1}ms", ms)
        }
    } else {
        String::new()
    };

    let left = format!(" NodeDB CLI | {}:{}", app.host, app.port);
    let right = format!("fmt:{}{} ", fmt, timing);
    let pad = area
        .width
        .saturating_sub(left.len() as u16 + right.len() as u16);

    let line = Line::from(vec![
        Span::styled(&left, style.add_modifier(Modifier::BOLD)),
        Span::styled(" ".repeat(pad as usize), style),
        Span::styled(&right, style),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn render_results(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let p = if let Some(ref err) = app.error_message {
        Paragraph::new(err.as_str())
            .style(Style::default().fg(Color::Red))
            .wrap(Wrap { trim: false })
    } else if let Some(ref output) = app.result_output {
        Paragraph::new(output.as_str())
            .wrap(Wrap { trim: false })
            .scroll((app.scroll_offset, 0))
    } else {
        Paragraph::new("Type SQL ending with ; and press Enter. \\? for help, \\q to quit.")
            .style(Style::default().fg(Color::DarkGray))
    };
    frame.render_widget(p, area);
}

fn render_separator(frame: &mut Frame, area: ratatui::layout::Rect) {
    let line = "─".repeat(area.width as usize);
    let p = Paragraph::new(line).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(p, area);
}

fn render_input(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let prompt = Style::default()
        .fg(Color::Green)
        .add_modifier(Modifier::BOLD);
    let buf = app.input.buffer();

    let mut lines: Vec<Line> = Vec::new();
    if buf.is_empty() {
        lines.push(Line::from(Span::styled("ndb> ", prompt)));
    } else {
        for (i, text) in buf.split('\n').enumerate() {
            let pfx = if i == 0 { "ndb> " } else { "  -> " };
            lines.push(Line::from(vec![Span::styled(pfx, prompt), Span::raw(text)]));
        }
    }
    frame.render_widget(Paragraph::new(lines), area);

    let cx = cursor_col(buf, app.input.cursor());
    let cy = cursor_row(buf, app.input.cursor());
    frame.set_cursor_position((area.x + 5 + cx as u16, area.y + cy as u16));
}

fn input_line_count(app: &App) -> usize {
    app.input.buffer().split('\n').count().clamp(1, 6)
}

fn cursor_col(buffer: &str, byte_pos: usize) -> usize {
    let before = &buffer[..byte_pos];
    match before.rfind('\n') {
        Some(nl) => before.len() - nl - 1,
        None => before.len(),
    }
}

fn cursor_row(buffer: &str, byte_pos: usize) -> usize {
    buffer[..byte_pos].matches('\n').count()
}
