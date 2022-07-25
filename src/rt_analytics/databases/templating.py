from typing import Dict, List, Optional, Union
from jinja2 import Environment, FileSystemLoader

def generate_sql_from_template(
    template_file: str, template_dict: str
) -> Union[str, List[str]]:
    """
    Generate a SQL string from a template
    
    Args:
        template_file (str): Location of template file
        template_dict (str): Parameters to put into template
    
    Returns:
        Union[str, List[str]]: String representing the SQL code generated
    """

    # check the input filename
    file_split = template_file.rsplit("/", 1)
    if len(file_split) > 1:
        dir_split = file_split[-2]
    else:
        dir_split = "."
    file_name = file_split[-1]

    file_loader = FileSystemLoader(dir_split)
    env = Environment(loader=file_loader)

    template = env.get_template(file_name)

    if isinstance(template_dict, dict):
        # single file to generate
        output = template.render(template_dict)

    else:
        # multiple files to generate
        output = []
        for input_dict in template_dict:
            output.append(template.render(input_dict))

    return output


def generate_sql_files_from_template(
    template_file: str, template_dict: str, output_file: str
) -> Union[str, List[str]]:
    """
    Generate a SQL file from a template file
    
    Args:
        template_file (str): Location of template file
        template_dict (str): Parameters to put into template
        output_file (str): Filename to write file to. When multiple files need to be generated, a 3 digit number is added to the end of the output_file
    
    Returns:
        Union[str, List[str]]: Outputted filenames
    """

    # generate SQL strings
    sql_code = generate_sql_from_template(template_file, template_dict)

    if isinstance(sql_code, list):
        # multiple files
        file_name = []
        file_ext = "." + output_file.rsplit(".", 1)[-1]

        for counter, sql in enumerate(sql_code):
            # write to file
            current_file = output_file.replace(
                file_ext, "_{0:03d}{1:s}".format(counter + 1, file_ext)
            )

            file_name.append(current_file)
            with open(current_file, "w") as f:
                f.write(sql)
    else:
        # single file only
        file_name = output_file

        # write to file
        with open(file_name, "w") as f:
            f.write(sql_code)

    return file_name
