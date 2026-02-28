import csv

def write_delta_csv(rows, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)

        # First column must be operation
        writer.writerow([
            "operation",
            "id",
            "name",
            "email",
            "created_at",
            "updated_at",
            "is_deleted"
        ])

        for row in rows:
            operation = determine_operation(row)

            writer.writerow([
                operation,
                row.id,
                row.name,
                row.email,
                row.created_at,
                row.updated_at,
                row.is_deleted
            ])