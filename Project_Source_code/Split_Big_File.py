def split_file(file_path, size_limit_mb, output_prefix):
    chunk_size = size_limit_mb * 1024 * 1024  # Convert MB to bytes
    file_number = 1
    file_output = f"{output_prefix}_{file_number}.json"
    with open(file_path, 'r', encoding='utf-8') as f:  # Specify UTF-8 encoding here
        chunk = f.read(chunk_size)
        while chunk:
            with open(file_output, 'w', encoding='utf-8') as chunk_file:  # Specify UTF-8 encoding here
                chunk_file.write(chunk)
            file_number += 1
            file_output = f"{output_prefix}_{file_number}.json"
            chunk = f.read(chunk_size)

# Call the function with the appropriate parameters
split_file('C:/Users/Hangyu/Desktop/Project 1/yelp_data/yelp_data/yelp_academic_dataset_review.json', 1024, 'yelp_review_split')
split_file('C:/Users/Hangyu/Desktop/Project 1/yelp_data/yelp_data/yelp_academic_dataset_user.json', 1024, 'yelp_user_split')