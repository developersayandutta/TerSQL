#include <iostream>
#include <cstdlib>

int main() {
    std::cout << "Starting TerSQL setup...\n";

    // Step 1: Install requirements
    std::cout << "Installing dependencies from requirements.txt...\n";
    int install_status = system("pip install -r requirements.txt");

    if (install_status != 0) {
        std::cerr << "Failed to install dependencies.\n";
        return 1;
    }

    std::cout << "Dependencies installed successfully.\n";

    // Step 2: Run main.py
    std::cout << "Launching application...\n";
    int clear_status = system("cls");
    int run_status = system("python main.py");

    if (run_status != 0) {
        std::cerr << "Failed to run main.py\n";
        return 1;
    }
    return 0;
}