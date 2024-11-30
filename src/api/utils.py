def build_report_response(data: dict) -> dict:
    return {
        'request_id': data.get('request_id'),
        'status': data.get('status'),
        'report_content': data.get('report_content')
    }