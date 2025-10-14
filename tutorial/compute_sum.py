import csv

def compute_sum(folder, input1, input2, output):
    """
    Reads two CSV files (input1.csv, input2.csv),
    computes the elementwise sum of their numeric values,
    and writes the result to output.csv.
    Assumes both files have the same header row and structure.
    """
    faasr_get_file(local_file="input1.csv", remote_folder=folder, remote_file=input1)
    faasr_get_file(local_file="input2.csv", remote_folder=folder, remote_file=input2)
  
    with open("input1.csv", newline="") as f1, open("input2.csv", newline="") as f2:
        reader1 = csv.reader(f1)
        reader2 = csv.reader(f2)

        # Read headers
        header1 = next(reader1)
        header2 = next(reader2)

        if header1 != header2:
            raise ValueError("Input files have different headers!")

        # Prepare output
        with open("output.csv", "w", newline="") as fout:
            writer = csv.writer(fout)
            writer.writerow(header1)  # write header

            # Process row by row
            for row1, row2 in zip(reader1, reader2):
                summed_row = [int(a) + int(b) for a, b in zip(row1, row2)]
                writer.writerow(summed_row)

    faasr_put_file(local_file="output.csv", remote_folder=folder, remote_file=output)